# Diagnóstico: S3 Metrics com total=0

## O que os logs mostram:

```
[GenericODMUDF] Métricas enviadas ao Kafka
[S3-METRICS] Snapshot resumo: total=0 ok=0 err=0 durMs=0 avg=0 rules=0 ruleset=unknown
```

## Análise:

✅ **S3 Metrics está funcionando corretamente**
- Está inicializado
- Está configurado com bucket/prefix/region
- Está tentando enviar relatórios

❌ **Problema: Nenhuma execução ODM foi registrada**
- `total=0` significa que `S3Metrics.recordExecution()` nunca foi chamado
- Ou o job terminou antes de processar os registros

## Causas Possíveis:

### 1. Job terminou antes de processar dados (MAIS PROVÁVEL)
O log mostra que as métricas foram enviadas, mas `total=0`. Isso acontece quando:
- O job foi interrompido antes de executar a UDF ODM
- Houve erro na leitura dos dados de entrada
- O DataFrame estava vazio

**Solução:** Verifique os logs anteriores para ver se houve:
- Erro ao ler o arquivo de entrada
- DataFrame vazio
- Erro ao executar a UDF

### 2. UDF não foi executada
Se o DataFrame não teve a coluna `odm_output` criada, a UDF não foi chamada.

**Solução:** Verifique se esta linha foi executada:
```python
df_result = df_with_input.withColumn("odm_output", expr("execute_odm(odm_input)"))
```

### 3. JAR não foi carregado corretamente
Se o JAR com o código atualizado não foi usado, a versão antiga (sem S3Metrics) pode estar rodando.

**Solução:** 
- Recompile o projeto: `mvn clean package`
- Faça upload do novo JAR para o S3
- Certifique-se de que o Glue está usando o JAR correto

## Como Verificar:

### 1. Verifique se o job processou registros:

Procure nos logs por:
```
📊 ESTATÍSTICAS DE EXECUÇÃO
  Total processado:     X registros
```

Se `X = 0`, o problema é que nenhum registro foi processado.

### 2. Verifique se a UDF foi chamada:

Procure por logs como:
```
[GenericODMUDF] Executando regra...
```

Se não aparecer, a UDF não foi chamada.

### 3. Verifique o JAR usado:

Nos logs do início do job, procure por:
```
📥 Baixando JARs do S3...
  ✅ Ruleset: /tmp/bre_visaodorelacionamentobancario.jar (X.X MB)
```

Confirme que o tamanho do JAR está correto (deve ser ~39MB se for o JAR completo).

## Solução Definitiva:

### Passo 1: Recompilar e fazer upload do JAR

```bash
# No diretório do projeto
mvn clean package

# Fazer upload para S3
aws s3 cp target/odm-embarcado-udf-1.0.0.jar s3://bre-laboratorio/embarcado/jars/
```

### Passo 2: Verificar se o script Python está correto

Certifique-se de que estas linhas estão no script:

```python
# Após criar o spark
if 'S3_METRICS_BUCKET' in os.environ:
    jvm = spark.sparkContext._jvm
    jvm.System.setProperty("S3_METRICS_BUCKET", os.environ['S3_METRICS_BUCKET'])
    jvm.System.setProperty("S3_METRICS_PREFIX", os.environ['S3_METRICS_PREFIX'])
    jvm.System.setProperty("S3_METRICS_REGION", os.environ['S3_METRICS_REGION'])
```

### Passo 3: Executar o job e verificar logs

Procure por:
```
[S3-METRICS] Inicializado: bucket=bre-laboratorio, prefix=embarcado/metricas/, region=sa-east-1
```

E depois:
```
[S3-METRICS] Snapshot resumo: total=XXXX ok=XXXX err=0
```

Se `total > 0`, está funcionando!

## Teste Rápido:

Para testar se o S3Metrics está funcionando, crie um job simples que processa apenas 10 registros:

```python
# Limitar para teste
df_input = df_input.limit(10)
```

Execute e verifique se `total=10` aparece nos logs.

## Logs Esperados (Funcionando Corretamente):

```
📊 S3 METRICS CONFIGURADO
  Bucket: bre-laboratorio
  Prefix: embarcado/metricas/
  Region: sa-east-1

[S3-METRICS] Inicializado: bucket=bre-laboratorio, prefix=embarcado/metricas/, region=sa-east-1

... processamento ...

📊 ESTATÍSTICAS DE EXECUÇÃO
  Total processado:     10,000 registros
  Tempo total:          120.45s

[S3-METRICS] Snapshot resumo: total=10000 ok=9995 err=5 durMs=50000 avg=5 rules=50000 ruleset=/bre_visaodorelacionamentobancario/1.0/elege_faturamento
[S3-METRICS] Relatórios ILMT enviados para S3 (decisions=10000)
[S3-METRICS] Arquivo enviado: s3://bre-laboratorio/embarcado/metricas/2026/03/05/15/ilmt-report-1709658379121.xml
[S3-METRICS] Arquivo enviado: s3://bre-laboratorio/embarcado/metricas/2026/03/05/15/custom-report-1709658379121.xml
```

## Conclusão:

O S3Metrics está **funcionando corretamente**, mas não recebeu nenhuma execução para registrar. 

**Próximo passo:** Verifique por que o job não processou registros ODM. Procure nos logs por erros antes da linha "[S3-METRICS] Snapshot resumo".