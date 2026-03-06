package br.com.itau.odm.embarcado;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import ilog.rules.res.model.IlrPath;
import ilog.rules.res.session.*;
import ilog.rules.res.session.config.IlrPersistenceType;
import ilog.rules.res.session.config.IlrSessionFactoryConfig;
import ilog.rules.res.session.config.IlrXUConfig;
import org.apache.spark.sql.api.java.UDF1;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * UDF Genérica para execução de regras ODM.
 * Funciona com qualquer XOM usando Reflection pura.
 * Integrada com KafkaMetrics (métricas) e S3Metrics (resultados).
 */
public class GenericODMUDF implements UDF1<String, String>, Serializable {
    
    private static final long serialVersionUID = 1L;
    private static final Gson gson = new Gson();
    private static FacadeSessionFactory sessionFactory;
    
    static {
        try {
            // Configurar para usar XU com persistência em MEMORY (carrega RuleApps do classpath/JARs)
            IlrSessionFactoryConfig config = FacadeSessionFactory.createDefaultConfig();
            IlrXUConfig xuConfig = config.getXUConfig();
            
            // Desabilita auto-flush de logs
            xuConfig.setLogAutoFlushEnabled(false);
            
            // Configura persistência em MEMORY ao invés de FILE
            xuConfig.getPersistenceConfig().setPersistenceType(IlrPersistenceType.MEMORY);
            xuConfig.getManagedXOMPersistenceConfig().setPersistenceType(IlrPersistenceType.MEMORY);
            
            sessionFactory = new FacadeSessionFactory(config);
            
            // Inicializar S3Metrics (opcional - não bloqueia se não configurado)
            S3Metrics.initIfNeeded();
            
            System.out.println("[GenericODMUDF] Inicializada com integração Kafka + S3 (modo XU MEMORY)");
            System.out.println("[GenericODMUDF] S3 Status: " + (S3Metrics.isReady() ? "ATIVO" : "DESABILITADO"));
        } catch (Exception e) {
            throw new RuntimeException("Erro ao inicializar GenericODMUDF", e);
        }
    }
    
    @Override
    public String call(String inputJson) throws Exception {
        long startTime = System.nanoTime();
        
        try {
            Map<String, Object> inputData = gson.fromJson(
                inputJson, new TypeToken<Map<String, Object>>() {}.getType());
            
            Map<String, Object> config = (Map<String, Object>) inputData.get("__config__");
            if (config == null) {
                throw new IllegalArgumentException("Configuração '__config__' não encontrada");
            }
            
            // DEBUG: Imprimir config completo
            System.out.println("[GenericODMUDF-DEBUG] Config recebido: " + config);
            System.out.println("[GenericODMUDF-DEBUG] Config keys: " + config.keySet());
            
            String rulesetPath = (String) config.get("ruleset_path");
            System.out.println("[GenericODMUDF-DEBUG] rulesetPath extraído: '" + rulesetPath + "'");
            String inputClassName = (String) config.get("input_class");
            String inputParamName = (String) config.get("input_param_name");
            List<String> outputParamNames = (List<String>) config.get("output_param_names");
            Map<String, String> typeMapping = (Map<String, String>) config.get("type_mapping");
            
            Map<String, Object> inputObjectData = (Map<String, Object>) inputData.get("data");
            
            Object inputObject = createObjectFromData(
                inputClassName, inputObjectData, 
                typeMapping != null ? typeMapping : new HashMap<>()
            );
            
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
            
            // Coletar métricas de execução usando reflection (mesmo método do FacadeStatelessSession)
            int rulesFired = tryGetTotalRulesFired(response);
            
            // DEBUG: Verificar se S3Metrics está pronto
            System.out.println("[GenericODMUDF-DEBUG] Antes de recordExecution - S3Metrics.isReady(): " + S3Metrics.isReady());
            System.out.println("[GenericODMUDF-DEBUG] rulesetPath: " + rulesetPath + ", executionTimeMs: " + executionTimeMs + ", rulesFired: " + rulesFired);
            
            // Enviar métricas ILMT para S3 (se configurado)
            S3Metrics.recordExecution(rulesetPath, executionTimeMs, rulesFired, true);
            
            System.out.println("[GenericODMUDF-DEBUG] Depois de recordExecution - chamada concluída");
            
            return gson.toJson(outputData);
            
        } catch (Exception e) {
            long durationMs = (System.nanoTime() - startTime) / 1_000_000L;
            System.err.println("[GenericODMUDF] Erro: " + e.getMessage());
            
            // Registrar erro nas métricas ILMT
            Map<String, Object> errorInputData = gson.fromJson(inputJson, new TypeToken<Map<String, Object>>() {}.getType());
            Map<String, Object> errorConfig = (Map<String, Object>) errorInputData.get("__config__");
            String errorRulesetPath = errorConfig != null ? (String) errorConfig.get("ruleset_path") : "unknown";
            S3Metrics.recordExecution(errorRulesetPath, durationMs, 0, false);
            
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", e.getMessage());
            errorResponse.put("errorType", e.getClass().getSimpleName());
            errorResponse.put("__ExecutionTimeMs__", durationMs);
            
            return gson.toJson(errorResponse);
        }
    }
    
    private Object createObjectFromData(String className, Map<String, Object> data,
                                       Map<String, String> typeMapping) throws Exception {
        Class<?> clazz = Class.forName(className);
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
    
    private void invokeSetter(Object obj, String setterName, Object value) throws Exception {
        Method setter = null;
        for (Method method : obj.getClass().getMethods()) {
            if (method.getName().equals(setterName) && method.getParameterCount() == 1) {
                setter = method;
                break;
            }
        }
        if (setter == null) {
            return; // Setter não encontrado, ignorar silenciosamente
        }
        
        Class<?> paramType = setter.getParameterTypes()[0];
        Object convertedValue = convertValue(value, paramType);
        setter.invoke(obj, convertedValue);
    }
    
    private Object convertValue(Object value, Class<?> targetType) {
        if (value == null || targetType.isInstance(value)) return value;
        
        // Se o valor é um número (Double do Gson), converter diretamente
        if (value instanceof Number) {
            Number numValue = (Number) value;
            if (targetType == Double.class || targetType == double.class)
                return numValue.doubleValue();
            if (targetType == Integer.class || targetType == int.class)
                return numValue.intValue();
            if (targetType == Long.class || targetType == long.class)
                return numValue.longValue();
            if (targetType == Float.class || targetType == float.class)
                return numValue.floatValue();
        }
        
        // Fallback para conversão via String
        String valueStr = value.toString();
        if (targetType == Double.class || targetType == double.class)
            return Double.parseDouble(valueStr);
        if (targetType == Integer.class || targetType == int.class)
            return Integer.parseInt(valueStr.contains(".") ? valueStr.substring(0, valueStr.indexOf('.')) : valueStr);
        if (targetType == Long.class || targetType == long.class)
            return Long.parseLong(valueStr.contains(".") ? valueStr.substring(0, valueStr.indexOf('.')) : valueStr);
        if (targetType == Boolean.class || targetType == boolean.class)
            return Boolean.parseBoolean(valueStr);
        if (targetType == Float.class || targetType == float.class)
            return Float.parseFloat(valueStr);
        if (targetType == String.class)
            return valueStr;
        
        return value;
    }
    
    private Map<String, Object> extractObjectData(Object obj) {
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
                            processedList.add(isPrimitiveOrWrapper(item.getClass()) ? 
                                item : extractObjectData(item));
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
            System.err.println("[GenericODMUDF] Erro ao extrair dados: " + e.getMessage());
        }
        return result;
    }
    
    private boolean isPrimitiveOrWrapper(Class<?> clazz) {
        return clazz.isPrimitive() || clazz == String.class || clazz == Integer.class ||
               clazz == Long.class || clazz == Double.class || clazz == Float.class ||
               clazz == Boolean.class || clazz == Character.class || 
               clazz == Byte.class || clazz == Short.class;
    }
    
    private String generateDecisionId(Map<String, Object> data) {
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
    
    /**
     * Tenta obter o número total de regras disparadas usando reflection.
     * Mesmo método usado no FacadeStatelessSession para compatibilidade com KafkaMetrics.
     */
    private static int tryGetTotalRulesFired(IlrSessionResponse response) {
        if (response == null) return 0;
        try {
            Object trace = response.getRulesetExecutionTrace();
            if (trace == null) return 0;
            
            Method m = trace.getClass().getMethod("getTotalRulesFired");
            Object val = m.invoke(trace);
            if (val instanceof Number) {
                long lf = ((Number) val).longValue();
                return (lf <= Integer.MAX_VALUE) ? (int) lf : Integer.MAX_VALUE;
            }
        } catch (Exception ignore) {
            // Se falhar, retorna 0 (não deve quebrar a execução)
        }
        return 0;
    }
    
    public static void shutdown() {
        if (sessionFactory != null) {
            sessionFactory.close();
            System.out.println("[GenericODMUDF] Métricas enviadas ao Kafka");
        }
        
        // S3Metrics envia automaticamente no shutdown (não precisa flush manual)
        if (S3Metrics.isReady()) {
            System.out.println("[GenericODMUDF] S3Metrics ativo - métricas serão enviadas no shutdown");
        }
    }
}

// Made with Bob
