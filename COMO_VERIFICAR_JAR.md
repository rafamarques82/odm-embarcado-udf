# Como Verificar se o Código Está no JAR

## Método 1: Listar classes no JAR

```bash
jar -tf target/odm-embarcado-udf-1.0.0.jar | grep "br/com/itau/odm/embarcado"
```

Deve mostrar:
```
br/com/itau/odm/embarcado/GenericODMUDF.class
br/com/itau/odm/embarcado/S3Metrics.class
br/com/itau/odm/embarcado/KafkaMetrics.class
br/com/itau/odm/embarcado/FacadeSessionFactory.class
br/com/itau/odm/embarcado/FacadeStatelessSession.class
```

## Método 2: Descompilar e verificar o código

```bash
# Extrair a classe
jar -xf target/odm-embarcado-udf-1.0.0.jar br/com/itau/odm/embarcado/GenericODMUDF.class

# Descompilar (se tiver jd-cli instalado)
jd-cli br/com/itau/odm/embarcado/GenericODMUDF.class

# Ou usar javap para ver os métodos
javap -c br/com/itau/odm/embarcado/GenericODMUDF.class | grep -A 5 "S3Metrics"
```

## Método 3: Procurar por string no JAR

```bash
# Procurar pela string "S3Metrics.recordExecution"
unzip -p target/odm-embarcado-udf-1.0.0.jar br/com/itau/odm/embarcado/GenericODMUDF.class | strings | grep -i s3metrics
```

## Método 4: Verificar tamanho do JAR

```bash
ls -lh target/odm-embarcado-udf-1.0.0.jar
```

**Tamanho esperado:** ~39MB (com todas as dependências)

Se for muito menor (ex: 50KB), significa que é só o código compilado sem dependências.

## Método 5: Verificar data de modificação

```bash
ls -l target/odm-embarcado-udf-1.0.0.jar
```

Compare com a data da última compilação.

## Método 6: Verificar no S3 (se já fez upload)

```bash
aws s3 ls s3://bre-laboratorio/embarcado/jars/ --recursive --human-readable
```

Compare o tamanho e data do arquivo no S3 com o local.

## O que procurar:

### No código fonte (GenericODMUDF.java):

Linha 109:
```java
S3Metrics.recordExecution(rulesetPath, executionTimeMs, rulesFired, true);
```

Linha 121:
```java
S3Metrics.recordExecution(errorRulesetPath, durationMs, 0, false);
```

### No bytecode (GenericODMUDF.class):

Deve ter referências a:
- `br/com/itau/odm/embarcado/S3Metrics`
- `recordExecution`

## Recompilar se necessário:

```bash
# Limpar e recompilar
mvn clean package

# Verificar o JAR gerado
ls -lh target/odm-embarcado-udf-1.0.0.jar

# Listar classes
jar -tf target/odm-embarcado-udf-1.0.0.jar | grep S3Metrics
```

Deve mostrar:
```
br/com/itau/odm/embarcado/S3Metrics.class
```

## Upload para S3:

```bash
aws s3 cp target/odm-embarcado-udf-1.0.0.jar \
  s3://bre-laboratorio/embarcado/jars/odm-embarcado-udf-1.0.0.jar \
  --region sa-east-1
```

## Verificar no Glue:

Após fazer upload, execute o job e verifique os logs:

```
[GenericODMUDF] Estatísticas S3: S3Metrics[sent=X, errors=0, executions=100000, ready=true]
```

Se `executions > 0`, o código está no JAR e funcionando!