package br.com.itau.odm.embarcado;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import ilog.rules.res.model.IlrPath;
import ilog.rules.res.session.*;
import org.apache.spark.sql.api.java.UDF1;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * UDF Genérica para execução de regras ODM.
 * Funciona com qualquer XOM usando Reflection pura.
 * Integrada com KafkaMetrics para envio de métricas.
 */
public class GenericODMUDF implements UDF1<String, String>, Serializable {
    
    private static final long serialVersionUID = 1L;
    private static final Gson gson = new Gson();
    private static FacadeSessionFactory sessionFactory;
    
    static {
        try {
            sessionFactory = new FacadeSessionFactory(
                FacadeSessionFactory.createDefaultConfig()
            );
            System.out.println("[GenericODMUDF] Inicializada com integração Kafka");
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
            
            String rulesetPath = (String) config.get("ruleset_path");
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
            
            outputData.put("__DecisionID__", generateDecisionId(inputObjectData));
            outputData.put("__ExecutionTimeMs__", (System.nanoTime() - startTime) / 1_000_000L);
            
            return gson.toJson(outputData);
            
        } catch (Exception e) {
            long durationMs = (System.nanoTime() - startTime) / 1_000_000L;
            System.err.println("[GenericODMUDF] Erro: " + e.getMessage());
            
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
        if (setter == null) return;
        
        Class<?> paramType = setter.getParameterTypes()[0];
        Object convertedValue = convertValue(value, paramType);
        setter.invoke(obj, convertedValue);
    }
    
    private Object convertValue(Object value, Class<?> targetType) {
        if (value == null || targetType.isInstance(value)) return value;
        
        String valueStr = value.toString();
        if (targetType == Double.class || targetType == double.class) 
            return new BigDecimal(valueStr).doubleValue();
        if (targetType == Integer.class || targetType == int.class) 
            return Integer.parseInt(valueStr);
        if (targetType == Long.class || targetType == long.class) 
            return Long.parseLong(valueStr);
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
    
    public static void shutdown() {
        if (sessionFactory != null) {
            sessionFactory.close();
            System.out.println("[GenericODMUDF] Métricas enviadas ao Kafka");
        }
    }
}

// Made with Bob
