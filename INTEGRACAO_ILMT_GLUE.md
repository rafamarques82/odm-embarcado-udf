# Integração ILMT com AWS Glue

## 📋 Resumo

Este documento explica como o script Glue `glue_job_credito_pj_com_ilmt.py` envia automaticamente métricas ILMT (IBM License Metric Tool) para S3 após processar decisões ODM.

## 🎯 O Que Foi Adicionado

### 1. Classe Java: `S3MetricsHelper`

**Localização**: `src/main/java/br/com/itau/odm/embarcado/S3MetricsHelper.java`

**Função**: Classe utilitária com método estático para enviar métricas ILMT para S3.

**Método Principal**:
```java
public static boolean sendIlmtMetrics(
    String bucketName,
    String prefix,
    String region,
    String rulesetPath,
    long totalDecisions,
    long startTimeMs,
    long endTimeMs
)
```

### 2. Script Glue Atualizado

**Arquivo**: `glue_job_credito_pj_com_ilmt.py`

**Mudanças**:
- ✅ Configuração de métricas ILMT (linhas 60-62)
- ✅ System Properties para S3 Metrics (linhas 165-170)
- ✅ Captura de timestamps (linhas 313-314, 326)
- ✅ Envio automático de métricas após processamento (linhas 382-408)

## 🔧 Configuração

### Variáveis no Script Glue

Edite estas variáveis no início do script:

```python
# --- ILMT Metrics ---
S3_METRICS_BUCKET = "bre-laboratorio"              # Bucket para métricas
S3_METRICS_PREFIX = "embarcado/ilmt-metrics/"      # Prefixo/pasta
S3_METRICS_REGION = "sa-east-1"                    # Região AWS
```

### Permissões IAM Necessárias

A role do Glue precisa de permissão para escrever no bucket de métricas:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:PutObjectAcl"
      ],
      "Resource": "arn:aws:s3:::bre-laboratorio/embarcado/ilmt-metrics/*"
    }
  ]
}
```

## 📊 Como Funciona

### Fluxo de Execução

```
1. Job Glue inicia
   ↓
2. Captura timestamp de início (start_time_ms)
   ↓
3. Processa N registros com ODM
   ↓
4. Captura timestamp de fim (end_time_ms)
   ↓
5. Chama S3MetricsHelper.sendIlmtMetrics() via py4j
   ↓
6. Classe Java gera XMLs ILMT
   ↓
7. Envia para S3 com particionamento por data/hora
   ↓
8. Job finaliza
```

### Código de Envio (Python)

```python
# Obter referência para a classe Java
S3MetricsHelper = jvm.br.com.itau.odm.embarcado.S3MetricsHelper

# Chamar método estático
ilmt_success = S3MetricsHelper.sendIlmtMetrics(
    S3_METRICS_BUCKET,      # "bre-laboratorio"
    S3_METRICS_PREFIX,      # "embarcado/ilmt-metrics/"
    S3_METRICS_REGION,      # "sa-east-1"
    RULESET_PATH,           # "/bre_visaodorelacionamentobancario/1.0/elege_faturamento"
    total_processed,        # 10000000 (exemplo)
    start_time_ms,          # 1709661600000
    end_time_ms             # 1709661930000
)

if ilmt_success:
    print("✅ Métricas ILMT enviadas!")
else:
    print("⚠️ Erro ao enviar métricas")
```

## 📁 Estrutura dos Arquivos no S3

Os arquivos são salvos com particionamento por data/hora:

```
s3://bre-laboratorio/
  └── embarcado/
      └── ilmt-metrics/
          └── 2026/
              └── 03/
                  └── 05/
                      └── 15/
                          ├── ilmt-report-1709661600000.xml
                          └── custom-report-1709661600000.xml
```

**Padrão**: `{prefix}{YYYY}/{MM}/{DD}/{HH}/{tipo}-report-{timestamp}.xml`

## 📄 Formato dos XMLs

### 1. ILMT Report (Formato IBM)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<DecisionServiceMetering>
  <RulesetPath>/bre_visaodorelacionamentobancario/1.0/elege_faturamento</RulesetPath>
  <StartTime>2026-03-05T15:00:00.000-0300</StartTime>
  <EndTime>2026-03-05T15:05:30.000-0300</EndTime>
  <TotalDecisions>10000000</TotalDecisions>
  <ProductName>IBM Operational Decision Manager</ProductName>
  <ProductVersion>8.12.0</ProductVersion>
</DecisionServiceMetering>
```

### 2. Custom Report (Formato Estendido)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<ODMExecutionSummary>
  <RulesetPath>/bre_visaodorelacionamentobancario/1.0/elege_faturamento</RulesetPath>
  <TimeWindow>
    <Start>2026-03-05T15:00:00.000-0300</Start>
    <End>2026-03-05T15:05:30.000-0300</End>
  </TimeWindow>
  <Executions>
    <Total>10000000</Total>
    <Success>10000000</Success>
    <Errors>0</Errors>
  </Executions>
  <Performance>
    <TotalDurationMs>330000</TotalDurationMs>
    <AverageDurationMs>0.03</AverageDurationMs>
  </Performance>
</ODMExecutionSummary>
```

## 🚀 Como Usar

### 1. Compilar o Projeto

```bash
cd /Users/rafaelmarques/Documents/ITAU/embarcado/Embarcado
mvn clean package
```

### 2. Upload do JAR para S3

```bash
aws s3 cp target/odm-embarcado-udf-1.0.0.jar \
  s3://bre-laboratorio/embarcado/jars/odm-embarcado-udf-1.0.0.jar
```

### 3. Criar/Atualizar Job Glue

- **Script**: `glue_job_credito_pj_com_ilmt.py`
- **Tipo**: Spark
- **Glue Version**: 4.0
- **Worker Type**: G.2X (recomendado)
- **Number of Workers**: 10-20

### 4. Configurar Parâmetros do Job

```
--JOB_NAME: credito-pj-com-ilmt
--extra-jars: s3://bre-laboratorio/embarcado/jars/odm-embarcado-udf-1.0.0.jar
```

### 5. Executar o Job

O job irá:
1. Processar os dados
2. Executar as regras ODM
3. **Enviar métricas ILMT automaticamente**
4. Salvar resultados

## 📊 Logs de Exemplo

```
================================================================================
📤 ENVIANDO MÉTRICAS ILMT PARA S3
================================================================================
[S3-HELPER] Iniciando envio de métricas ILMT...
[S3-HELPER] Bucket: bre-laboratorio, Prefix: embarcado/ilmt-metrics/, Region: sa-east-1
[S3-HELPER] Ruleset: /bre_visaodorelacionamentobancario/1.0/elege_faturamento, Decisions: 10000000
[S3-HELPER] ✅ ILMT report enviado: s3://bre-laboratorio/embarcado/ilmt-metrics/2026/03/05/15/ilmt-report-1709661600000.xml
[S3-HELPER] ✅ Custom report enviado: s3://bre-laboratorio/embarcado/ilmt-metrics/2026/03/05/15/custom-report-1709661600000.xml
[S3-HELPER] ✅ Métricas ILMT enviadas com sucesso!
  ✅ Métricas ILMT enviadas com sucesso!
     Bucket: s3://bre-laboratorio/embarcado/ilmt-metrics/
     Decisões: 10,000,000
```

## 🔍 Verificação

### Listar Arquivos no S3

```bash
aws s3 ls s3://bre-laboratorio/embarcado/ilmt-metrics/ --recursive
```

### Baixar e Visualizar

```bash
aws s3 cp s3://bre-laboratorio/embarcado/ilmt-metrics/2026/03/05/15/ilmt-report-1709661600000.xml .
cat ilmt-report-1709661600000.xml
```

## ⚠️ Troubleshooting

### Problema: "Class not found: S3MetricsHelper"

**Causa**: JAR não foi compilado com a nova classe ou não está no classpath.

**Solução**:
```bash
# Recompilar
mvn clean package

# Verificar se a classe está no JAR
jar tf target/odm-embarcado-udf-1.0.0.jar | grep S3MetricsHelper

# Re-upload para S3
aws s3 cp target/odm-embarcado-udf-1.0.0.jar s3://seu-bucket/jars/
```

### Problema: "Access Denied" ao escrever no S3

**Causa**: Role do Glue não tem permissão.

**Solução**: Adicionar política IAM (veja seção "Permissões IAM Necessárias").

### Problema: Métricas não aparecem no S3

**Causa**: Possível erro silencioso ou bucket/prefix incorreto.

**Solução**:
1. Verificar logs do CloudWatch
2. Confirmar variáveis `S3_METRICS_BUCKET` e `S3_METRICS_PREFIX`
3. Testar manualmente:
```python
# No console Python do Glue
S3MetricsHelper = jvm.br.com.itau.odm.embarcado.S3MetricsHelper
result = S3MetricsHelper.sendIlmtMetrics(
    "bre-laboratorio", 
    "test/", 
    "sa-east-1", 
    "/test", 
    100, 
    1709661600000, 
    1709661700000
)
print(result)  # Deve retornar True
```

## 📈 Benefícios

✅ **Automático**: Não precisa de intervenção manual  
✅ **Confiável**: Executa no driver após o job completar  
✅ **Simples**: Apenas uma chamada Java do Python  
✅ **Particionado**: Fácil de consultar por data/hora  
✅ **Compliant**: Formato ILMT oficial da IBM  
✅ **Auditável**: Todos os XMLs ficam armazenados no S3  

## 🔗 Arquivos Relacionados

- `src/main/java/br/com/itau/odm/embarcado/S3MetricsHelper.java` - Classe Java
- `glue_job_credito_pj_com_ilmt.py` - Script Glue completo
- `COMO_USAR_S3_METRICS_HELPER.md` - Documentação detalhada da classe
- `pom.xml` - Configuração Maven (inclui dependências AWS SDK)

## 📞 Suporte

Para dúvidas ou problemas:
1. Verificar logs do CloudWatch do job Glue
2. Consultar este documento
3. Verificar permissões IAM
4. Testar manualmente a classe `S3MetricsHelper`