package br.com.itau.odm.embarcado.server;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import ilog.rules.res.model.IlrPath;
import ilog.rules.res.session.*;
import ilog.rules.res.session.config.IlrPersistenceType;
import ilog.rules.res.session.config.IlrSessionFactoryConfig;
import ilog.rules.res.session.config.IlrXUConfig;

import br.com.itau.odm.embarcado.FacadeSessionFactory;

import java.io.*;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Servidor standalone Java 21 para execução de regras ODM 9.5, isolado da JVM
 * do Spark/Glue (que roda em Java 8/17). Sobe UMA VEZ POR EXECUTOR (não por
 * partição/registro) e mantém o XU (cache + pool de sessões) aquecido para
 * todas as requisições daquele executor, replicando o comportamento que a
 * UDF nativa (GenericODMUDF) tinha quando rodava embarcada na própria JVM Spark.
 *
 * Protocolo: uma linha JSON de entrada por requisição, uma linha JSON de saída
 * por resposta, via socket TCP em localhost. O mesmo payload que hoje é passado
 * para GenericODMUDF.call(String) é aceito aqui sem alteração.
 *
 * Uso:
 *   java -jar odm-embarcado-server-21.jar <porta> [arquivo-porta-pronta]
 *
 * O segundo argumento (opcional) é um arquivo que o servidor cria assim que
 * está pronto para aceitar conexões — usado pela UDF Python para detectar
 * "server pronto" sem precisar fazer polling na porta.
 */
public class OdmServer {

    private static final Gson gson = new Gson();
    private static FacadeSessionFactory sessionFactory;

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Uso: java -jar odm-embarcado-server-21.jar <porta> [arquivo-pronto] [idle-timeout-seg]");
            System.exit(2);
        }
        int port = Integer.parseInt(args[0]);
        String readyFile = args.length >= 2 ? args[1] : null;
        // Idle timeout: se nenhuma conexão nova chegar em N segundos, o processo
        // se encerra sozinho (evita processo zumbi se o executor morrer sem avisar).
        long idleTimeoutMs = (args.length >= 3 ? Long.parseLong(args[2]) : 600) * 1000L;

        System.out.println("[OdmServer] Iniciando (Java " + System.getProperty("java.version") + ")...");
        initSessionFactory();

        ExecutorService pool = Executors.newCachedThreadPool();
        AtomicBoolean running = new AtomicBoolean(true);
        final long[] lastActivity = { System.currentTimeMillis() };

        try (ServerSocket serverSocket = new ServerSocket(port, 512, InetAddress.getByName("127.0.0.1"))) {
            System.out.println("[OdmServer] Ouvindo em 127.0.0.1:" + port);

            if (readyFile != null) {
                Files.writeString(Paths.get(readyFile), String.valueOf(port), StandardCharsets.UTF_8);
            }

            // Watchdog de idle-timeout, para não deixar processo pendurado
            // caso o executor Spark morra sem encerrar o daemon explicitamente.
            Thread watchdog = new Thread(() -> {
                while (running.get()) {
                    try {
                        Thread.sleep(30_000);
                        if (System.currentTimeMillis() - lastActivity[0] > idleTimeoutMs) {
                            System.out.println("[OdmServer] Idle timeout atingido, encerrando.");
                            running.set(false);
                            System.exit(0);
                        }
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }, "odm-server-watchdog");
            watchdog.setDaemon(true);
            watchdog.start();

            serverSocket.setSoTimeout(30_000);
            while (running.get()) {
                Socket client;
                try {
                    client = serverSocket.accept();
                } catch (java.net.SocketTimeoutException timeout) {
                    continue; // só volta pro loop e deixa o watchdog decidir se encerra
                }
                lastActivity[0] = System.currentTimeMillis();
                pool.submit(() -> handleClient(client, lastActivity));
            }
        } finally {
            pool.shutdownNow();
            if (sessionFactory != null) {
                sessionFactory.close();
            }
        }
    }

    private static void initSessionFactory() {
        try {
            IlrSessionFactoryConfig config = FacadeSessionFactory.createDefaultConfig();
            IlrXUConfig xuConfig = config.getXUConfig();

            xuConfig.setLogAutoFlushEnabled(false);
            xuConfig.getPersistenceConfig().setPersistenceType(IlrPersistenceType.MEMORY);
            xuConfig.getManagedXOMPersistenceConfig().setPersistenceType(IlrPersistenceType.MEMORY);

            sessionFactory = new FacadeSessionFactory(config);
            System.out.println("[OdmServer] SessionFactory inicializada (modo XU MEMORY)");
        } catch (Exception e) {
            throw new RuntimeException("Erro ao inicializar OdmServer", e);
        }
    }

    /**
     * Um socket pode carregar múltiplas requisições em sequência (keep-alive),
     * para a UDF Python reaproveitar a mesma conexão em vez de abrir uma nova
     * por registro. Protocolo simples: uma linha JSON de request, uma linha
     * JSON de response; conexão fecha quando o cliente fecha o socket.
     */
    private static void handleClient(Socket client, long[] lastActivity) {
        try (client;
             BufferedReader reader = new BufferedReader(
                     new InputStreamReader(client.getInputStream(), StandardCharsets.UTF_8));
             BufferedWriter writer = new BufferedWriter(
                     new OutputStreamWriter(client.getOutputStream(), StandardCharsets.UTF_8))) {

            String line;
            while ((line = reader.readLine()) != null) {
                lastActivity[0] = System.currentTimeMillis();
                String responseJson = processRequest(line);
                // Escapa quebras de linha internas: a resposta já é um JSON válido
                // em uma única linha (Gson não emite newlines em toJson por padrão).
                writer.write(responseJson);
                writer.write("\n");
                writer.flush();
            }
        } catch (IOException e) {
            // Cliente desconectou/erro de rede — não é fatal para o servidor.
            System.err.println("[OdmServer] Conexão encerrada: " + e.getMessage());
        }
    }

    /**
     * Mesma lógica de GenericODMUDF.call(String), adaptada para retornar a
     * string de resultado em vez de implementar UDF1. Mantida 1:1 de propósito
     * para não introduzir divergência de comportamento entre os dois caminhos.
     */
    private static String processRequest(String inputJson) {
        long startTime = System.nanoTime();
        try {
            Map<String, Object> inputData = gson.fromJson(
                    inputJson, new TypeToken<Map<String, Object>>() {}.getType());

            Map<String, Object> config = (Map<String, Object>) inputData.get("__config__");
            if (config == null) {
                throw new IllegalArgumentException("Configuração '__config__' não encontrada");
            }

            String rulesetPath = (String) config.get("ruleset_path");
            String inputClassName = (String) config.get("input_class");
            String inputParamName = (String) config.get("input_param_name");
            List<String> outputParamNames = (List<String>) config.get("output_param_names");
            Map<String, String> typeMapping = (Map<String, String>) config.get("type_mapping");

            Map<String, Object> inputObjectData = (Map<String, Object>) inputData.get("data");

            Object inputObject = createObjectFromData(
                    inputClassName, inputObjectData,
                    typeMapping != null ? typeMapping : new HashMap<>());

            IlrSessionRequest request = sessionFactory.createRequest();
            request.setRulesetPath(IlrPath.parsePath(rulesetPath));
            request.setForceUptodate(true);

            Map<String, Object> inputParams = new HashMap<>();
            inputParams.put(inputParamName, inputObject);
            request.setInputParameters(inputParams);

            IlrStatelessSession session = sessionFactory.createStatelessSession();
            IlrSessionResponse response = session.execute(request);

            Map<String, Object> outputData = new HashMap<>();
            if (outputParamNames != null) {
                for (String paramName : outputParamNames) {
                    Object outputObject = response.getOutputParameters().get(paramName);
                    if (outputObject != null) {
                        outputData.put(paramName, extractObjectData(outputObject));
                    }
                }
            }

            long executionTimeMs = (System.nanoTime() - startTime) / 1_000_000L;
            outputData.put("__DecisionID__", generateDecisionId(inputObjectData));
            outputData.put("__ExecutionTimeMs__", executionTimeMs);

            return gson.toJson(outputData);

        } catch (Exception e) {
            long durationMs = (System.nanoTime() - startTime) / 1_000_000L;
            System.err.println("[OdmServer] Erro: " + e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", e.getMessage());
            errorResponse.put("errorType", e.getClass().getSimpleName());
            errorResponse.put("__ExecutionTimeMs__", durationMs);

            return gson.toJson(errorResponse);
        }
    }

    // ===== Helpers reaproveitados de GenericODMUDF (reflection de entrada/saída) =====

    private static Object createObjectFromData(String className, Map<String, Object> data,
                                                Map<String, String> typeMapping) throws Exception {
        Class<?> clazz = Class.forName(className, true, Thread.currentThread().getContextClassLoader());
        Object obj = clazz.getDeclaredConstructor().newInstance();

        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String fieldName = entry.getKey();
            Object value = entry.getValue();
            String setterName = "set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);

            if (value instanceof List) {
                List<?> listData = (List<?>) value;
                if (!listData.isEmpty() && listData.get(0) instanceof Map) {
                    String childClassName = typeMapping.get(fieldName + "_class");
                    if (childClassName != null) {
                        List<Object> childObjects = new ArrayList<>();
                        for (Object item : listData) {
                            childObjects.add(createObjectFromData(childClassName, (Map) item, typeMapping));
                        }
                        invokeSetter(obj, setterName, childObjects);
                        continue;
                    }
                }
            }

            if (value instanceof Map) {
                String childClassName = typeMapping.get(fieldName + "_class");
                if (childClassName != null) {
                    invokeSetter(obj, setterName, createObjectFromData(childClassName, (Map) value, typeMapping));
                    continue;
                }
            }

            invokeSetter(obj, setterName, value);
        }
        return obj;
    }

    private static void invokeSetter(Object obj, String setterName, Object value) throws Exception {
        Method setter = null;
        for (Method method : obj.getClass().getMethods()) {
            if (method.getName().equals(setterName) && method.getParameterCount() == 1) {
                setter = method;
                break;
            }
        }
        if (setter == null) {
            return;
        }
        Class<?> paramType = setter.getParameterTypes()[0];
        Object convertedValue = convertValue(value, paramType);
        setter.invoke(obj, convertedValue);
    }

    private static Object convertValue(Object value, Class<?> targetType) {
        if (value == null || targetType.isInstance(value)) return value;

        if (value instanceof Number) {
            Number numValue = (Number) value;
            if (targetType == Double.class || targetType == double.class) return numValue.doubleValue();
            if (targetType == Integer.class || targetType == int.class) return numValue.intValue();
            if (targetType == Long.class || targetType == long.class) return numValue.longValue();
            if (targetType == Float.class || targetType == float.class) return numValue.floatValue();
        }

        String valueStr = value.toString();
        if (targetType == Double.class || targetType == double.class) return Double.parseDouble(valueStr);
        if (targetType == Integer.class || targetType == int.class)
            return Integer.parseInt(valueStr.contains(".") ? valueStr.substring(0, valueStr.indexOf('.')) : valueStr);
        if (targetType == Long.class || targetType == long.class)
            return Long.parseLong(valueStr.contains(".") ? valueStr.substring(0, valueStr.indexOf('.')) : valueStr);
        if (targetType == Boolean.class || targetType == boolean.class) return Boolean.parseBoolean(valueStr);
        if (targetType == Float.class || targetType == float.class) return Float.parseFloat(valueStr);
        if (targetType == String.class) return valueStr;

        return value;
    }

    private static Map<String, Object> extractObjectData(Object obj) {
        Map<String, Object> result = new HashMap<>();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

        try {
            for (Method method : obj.getClass().getMethods()) {
                if (method.getName().startsWith("get") && method.getParameterCount() == 0) {
                    String key = method.getName().substring(3);
                    key = key.substring(0, 1).toLowerCase() + key.substring(1);
                    if ("class".equals(key)) continue;

                    Object value = method.invoke(obj);
                    if (value == null) {
                        result.put(key, null);
                    } else if (value instanceof List) {
                        List<Object> processedList = new ArrayList<>();
                        for (Object item : (List<?>) value) {
                            processedList.add(isPrimitiveOrWrapper(item.getClass()) ? item : extractObjectData(item));
                        }
                        result.put(key, processedList);
                    } else if (value instanceof Date) {
                        result.put(key, dateFormat.format((Date) value));
                    } else if (value instanceof Double) {
                        result.put(key, new BigDecimal((Double) value)
                                .setScale(2, BigDecimal.ROUND_HALF_UP).toPlainString());
                    } else if (isPrimitiveOrWrapper(value.getClass())) {
                        result.put(key, value);
                    } else {
                        result.put(key, extractObjectData(value));
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("[OdmServer] Erro ao extrair dados: " + e.getMessage());
        }
        return result;
    }

    private static boolean isPrimitiveOrWrapper(Class<?> clazz) {
        return clazz.isPrimitive() || clazz == String.class || clazz == Integer.class ||
               clazz == Long.class || clazz == Double.class || clazz == Float.class ||
               clazz == Boolean.class || clazz == Character.class ||
               clazz == Byte.class || clazz == Short.class;
    }

    private static String generateDecisionId(Map<String, Object> data) {
        String id = "UNKNOWN";
        for (String key : Arrays.asList("id", "numero", "codigo", "cnpj", "cpf",
                "num_cnpj_raiz", "numero_proposta")) {
            if (data.containsKey(key)) {
                id = String.valueOf(data.get(key));
                break;
            }
        }
        return id + "-" + new SimpleDateFormat("yyyy-MM-dd").format(new Date());
    }
}
