# 📊 Guia de Integração ILMT - AWS Glue Job

## Visão Geral

Este guia documenta a integração do **IBM License Metric Tool (ILMT)** no AWS Glue Job para rastreamento automático de uso de licenças ODM (Operational Decision Manager).

## 🎯 Objetivo

Gerar e enviar automaticamente relatórios de métricas de uso do ODM para S3, compatíveis com o formato ILMT da IBM, permitindo auditoria e compliance de licenças.

---

## 🏗️ Arquitetura

```
┌─────────────────────────────────────────────────────────────┐
│                    AWS Glue Job (Python)                     │
│                                                              │
│  1. Processa decisões ODM via GenericODMUDF                 │
│  2. Coleta métricas (total decisões, timestamps)            │
│  3. Chama S3MetricsHelper.sendIlmtMetrics() (Java)          │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              S3MetricsHelper (Java Class)                    │
│                                                              │
│  1. Cria DecisionMeteringReport                             │
│  2. Gera ILMT XML oficial (via writeILMTFile())             │
│  3. Gera Custom XML (métricas adicionais)                   │
│  4. Envia ambos para S3 com particionamento temporal        │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                         Amazon S3                            │
│                                                              │
│  s3://bucket/prefix/yyyy/MM/dd/HH/                          │
│    ├── ilmt-report-{timestamp}.xml    (formato IBM)         │
│    └── custom-report-{timestamp}.xml  (métricas extras)     │
└─────────────────────────────────────────────────────────────┘
```

---

## 📦 Componentes

### 1. **Python (Glue Job)**
- **Arquivo**: `glue_job_credito_pj_com_ilmt.py`
- **Responsabilidade**: 
  - Executar regras ODM
  - Coletar métricas de execução
  - Invocar método Java para envio ILMT

### 2. **Java Classes**
Localizadas em `src/main/java/br/com/itau/odm/embarcado/`:

#### `S3MetricsHelper.java`
```java
public static boolean sendIlmtMetrics(
    String bucketName,      // Bucket S3 de destino
    String prefix,          // Prefixo/path no S3
    String region,          // Região AWS (ex: sa-east-1)
    String rulesetPath,     // Path do ruleset ODM
    long totalDecisions,    // Total de decisões executadas
    long startTimeMs,       // Timestamp início (milissegundos)
    long endTimeMs          // Timestamp fim (milissegundos)
)
```

**Funcionalidades**:
- Cria instância de `DecisionMeteringReport`
- Gera XML ILMT oficial usando bibliotecas IBM
- Gera XML customizado com métricas adicionais
- Faz upload para S3 com particionamento temporal

#### `DecisionMetering.java`
- Gerencia criação de relatórios de uso
- Wrapper para `DecisionMeteringReport`

#### `DecisionMeteringReport.java`
- Gera relatórios no formato ILMT oficial
- Usa bibliotecas IBM: `com.ibm.license.metric.*`
- Calcula métricas: `MILLION_MONTHLY_DECISIONS` ou `THOUSAND_MONTHLY_ARTIFACTS`

---

## 📊 Métricas ILMT

### Tipos de Métricas

| Métrica | Quando Usar | Granularidade | Exemplo |
|---------|-------------|---------------|---------|
| **MILLION_MONTHLY_DECISIONS** | >= 1 milhão de decisões | 3 decimais | 4.999 = 4M + 999k decisões |
| **THOUSAND_MONTHLY_ARTIFACTS** | < 1 milhão de decisões | 3 decimais | 2.503 = 2k + 503 decisões |

### Cálculo

```python
# Python (Glue Job)
total_processed = 4_999_111  # Total de decisões

# Java (DecisionMeteringReport)
double doubleMDecisions = totalDecisions / 1_000_000.0;
doubleMDecisions = Math.round(doubleMDecisions * 1000d) / 1000d;  // 3 decimais

if (doubleMDecisions >= 1.0) {
    metricName = "MILLION_MONTHLY_DECISIONS";
    value = 4.999;  // milhões
} else {
    metricName = "THOUSAND_MONTHLY_ARTIFACTS";
    value = doubleKDecisions;  // milhares
}
```

---

## 📄 Formatos de Relatório

### 1. ILMT Report (Formato IBM Oficial)

**Arquivo**: `ilmt-report-{timestamp}.xml`

```xml
<SchemaVersion>2.1.1</SchemaVersion>
<SoftwareIdentity>
  <PersistentId>b1a07d4dc0364452aa6206bb6584061d</PersistentId>
  <Name>IBM Operational Decision Manager Server</Name>
  <InstanceId>/usr/IBM/TAMIT</InstanceId>
</SoftwareIdentity>
<Metric logTime="2024-03-09T12:30:00.000-0300">
  <Type>MILLION_MONTHLY_DECISIONS</Type>
  <SubType></SubType>
  <Value>4.999</Value>
  <Period>
    <StartTime>2024-03-09T12:00:00.000-0300</StartTime>
    <EndTime>2024-03-09T12:30:00.000-0300</EndTime>
  </Period>
</Metric>
```

**Características**:
- Formato oficial IBM para auditoria
- Contém `PersistentId` do produto ODM
- Compatível com ferramentas IBM License Metric Tool
- Gerado via `LicenseMetricLogger` (biblioteca IBM)

### 2. Custom Report (Métricas Adicionais)

**Arquivo**: `custom-report-{timestamp}.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<ODMExecutionSummary>
  <RulesetPath>/bre_visaodorelacionamentobancario/1.0/elege_faturamento</RulesetPath>
  <TimeWindow>
    <Start>2024-03-09T12:00:00.000-0300</Start>
    <End>2024-03-09T12:30:00.000-0300</End>
  </TimeWindow>
  <Executions>
    <Total>4999111</Total>
    <Success>4999111</Success>
    <Errors>0</Errors>
  </Executions>
  <Performance>
    <TotalDurationMs>1800000</TotalDurationMs>
    <AverageDurationMs>0.36</AverageDurationMs>
  </Performance>
</ODMExecutionSummary>
```

**Características**:
- Métricas customizadas para análise interna
- Inclui performance (duração média por decisão)
- Informações de sucesso/erro
- Path do ruleset executado

---

## 🗂️ Estrutura S3

### Particionamento Temporal

```
s3://bre-laboratorio/embarcado/ilmt-reports/
├── 2024/
│   ├── 03/
│   │   ├── 09/
│   │   │   ├── 12/
│   │   │   │   ├── ilmt-report-1709996400000.xml
│   │   │   │   └── custom-report-1709996400000.xml
│   │   │   ├── 13/
│   │   │   │   ├── ilmt-report-1710000000000.xml
│   │   │   │   └── custom-report-1710000000000.xml
```

**Benefícios**:
- ✅ Fácil navegação por data/hora
- ✅ Otimizado para queries Athena/Glue Catalog
- ✅ Lifecycle policies por período
- ✅ Auditoria temporal simplificada

---

## ⚙️ Configuração

### No Glue Job (Python)

```python
# Configuração S3 para ILMT (após processamento)
S3_ILMT_BUCKET = "bre-laboratorio"
S3_ILMT_PREFIX = "embarcado/ilmt-reports/"
S3_ILMT_REGION = "sa-east-1"

# Chamar após processar todas as decisões
success = jvm.br.com.itau.odm.embarcado.S3MetricsHelper.sendIlmtMetrics(
    S3_ILMT_BUCKET,      # bucketName
    S3_ILMT_PREFIX,      # prefix
    S3_ILMT_REGION,      # region
    RULESET_PATH,        # rulesetPath
    total_processed,     # totalDecisions (long)
    job_start_ms,        # startTimeMs (long)
    job_end_ms           # endTimeMs (long)
)
```

### Variáveis de Ambiente (Opcional)

```bash
# Se necessário configurar via environment
export ILMT_S3_BUCKET="bre-laboratorio"
export ILMT_S3_PREFIX="embarcado/ilmt-reports/"
export ILMT_S3_REGION="sa-east-1"
```

---

## 🔐 Permissões IAM

### Policy Mínima Necessária

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
      "Resource": [
        "arn:aws:s3:::bre-laboratorio/embarcado/ilmt-reports/*"
      ]
    }
  ]
}
```

### Role do Glue Job

Certifique-se que a role do Glue Job (`AWSGlueServiceRole-*`) tenha:
1. Permissões S3 acima
2. Acesso ao bucket de JARs (para download)
3. Acesso ao bucket de output (para resultados)

---

## 🚀 Fluxo de Execução

### 1. Inicialização
```python
# Download JARs do S3
s3_client.download_file(bucket, key, local_path)

# Inicializar Spark com JARs
jsc.addJar(XOM_PATH_LOCAL)
jsc.addJar(RULESET_JAR_LOCAL)
jsc.addJar(UDF_JAR_LOCAL)  # Contém S3MetricsHelper
```

### 2. Processamento
```python
# Executar regras ODM
df_result = df_with_input.withColumn(
    "odm_output", 
    expr("execute_odm(odm_input)")
)

# Coletar métricas
total_processed = df_result.count()
elapsed_time = time.time() - start_time
```

### 3. Envio ILMT
```python
# Calcular timestamps
job_start_ms = int(start_time * 1000)
job_end_ms = int((start_time + elapsed_time) * 1000)

# Enviar relatórios
success = jvm.br.com.itau.odm.embarcado.S3MetricsHelper.sendIlmtMetrics(
    S3_ILMT_BUCKET,
    S3_ILMT_PREFIX,
    S3_ILMT_REGION,
    RULESET_PATH,
    total_processed,
    job_start_ms,
    job_end_ms
)
```

### 4. Verificação
```python
if success:
    print("✅ Relatórios ILMT enviados com sucesso!")
    # Logs mostram paths dos arquivos gerados
else:
    print("⚠️ Falha ao enviar relatórios ILMT")
```

---

## 📈 Exemplo de Output

### Console do Glue Job

```
================================================================================
📊 GERANDO E ENVIANDO RELATÓRIO ILMT PARA S3
================================================================================
  📋 Informações do Relatório:
     Ruleset:        /bre_visaodorelacionamentobancario/1.0/elege_faturamento
     Total Decisões: 4,999,111
     Início:         2024-03-09T12:00:00
     Fim:            2024-03-09T12:30:00
     Duração:        1800.00s

  📤 Enviando para S3: s3://bre-laboratorio/embarcado/ilmt-reports/
[S3-HELPER] Iniciando envio de metricas ILMT...
[S3-HELPER] ILMT report enviado: s3://bre-laboratorio/embarcado/ilmt-reports/2024/03/09/12/ilmt-report-1709996400000.xml
[S3-HELPER] Custom report enviado: s3://bre-laboratorio/embarcado/ilmt-reports/2024/03/09/12/custom-report-1709996400000.xml
[S3-HELPER] Metricas ILMT enviadas com sucesso!
  ✅ Relatórios ILMT enviados com sucesso!
     📁 Bucket:  s3://bre-laboratorio
     📂 Prefix:  embarcado/ilmt-reports/
     📄 Arquivos:
        • ILMT Report:   embarcado/ilmt-reports/2024/03/09/12/ilmt-report-1709996400000.xml
        • Custom Report: embarcado/ilmt-reports/2024/03/09/12/custom-report-1709996400000.xml

  📊 Métricas ILMT:
     Métrica:  MILLION_MONTHLY_DECISIONS
     Valor:    4.999 milhões
```

---

## 🔍 Troubleshooting

### Problema: "Nenhum arquivo ILMT encontrado"

**Causa**: Diretório `/var/ibm/slmtags` não criado ou sem permissões

**Solução**:
```java
// S3MetricsHelper.java já cria o diretório automaticamente
Path ilmtDir = Paths.get("./var/ibm/slmtags");
Files.createDirectories(ilmtDir);
```

### Problema: "ERRO ao enviar metricas: Access Denied"

**Causa**: IAM role sem permissões S3

**Solução**:
1. Verificar policy da role do Glue Job
2. Adicionar permissão `s3:PutObject` no bucket de destino
3. Verificar bucket policy (não deve bloquear PutObject)

### Problema: "Total de decisoes invalido: 0"

**Causa**: Nenhuma decisão foi processada

**Solução**:
- Verificar se `df_result.count()` retorna > 0
- Verificar logs de execução ODM
- Confirmar que input tem dados válidos

### Problema: Relatórios não aparecem no S3

**Causa**: Erro silencioso ou path incorreto

**Solução**:
1. Verificar logs do Glue Job (CloudWatch)
2. Confirmar que `success = true` no retorno
3. Usar AWS CLI para listar:
```bash
aws s3 ls s3://bre-laboratorio/embarcado/ilmt-reports/ --recursive
```

---

## 📚 Referências

### Bibliotecas IBM

- **LicenseMetricLogger**: `com.ibm.license.metric.LicenseMetricLogger`
- **Metric**: `com.ibm.license.metric.Metric`
- **SoftwareIdentity**: `com.ibm.license.metric.SoftwareIdentity`

### Documentação IBM

- [IBM License Metric Tool](https://www.ibm.com/docs/en/license-metric-tool)
- [ODM Licensing](https://www.ibm.com/docs/en/odm/8.12.0?topic=manager-licensing)

### Código Fonte

- `src/main/java/br/com/itau/odm/embarcado/S3MetricsHelper.java`
- `src/main/java/br/com/itau/odm/embarcado/DecisionMetering.java`
- `src/main/java/br/com/itau/odm/embarcado/DecisionMeteringReport.java`
- `glue_job_credito_pj_com_ilmt.py`

---

## ✅ Checklist de Implementação

- [x] Classes Java implementadas (S3MetricsHelper, DecisionMetering, DecisionMeteringReport)
- [x] Integração no Glue Job Python
- [x] Configuração S3 (bucket, prefix, region)
- [x] Permissões IAM configuradas
- [x] Testes de envio para S3
- [x] Documentação completa
- [x] Logs detalhados de execução
- [x] Tratamento de erros

---

## 🎯 Próximos Passos

1. **Athena Integration**: Criar tabela Athena para query dos XMLs
2. **Dashboard**: Visualizar métricas ILMT no QuickSight
3. **Alertas**: CloudWatch Alarms para falhas de envio
4. **Agregação**: Script para consolidar métricas mensais
5. **Auditoria**: Processo automatizado de validação ILMT

---

**Última Atualização**: 2024-03-09  
**Versão**: 1.0  
**Autor**: Equipe Embarcado ODM