# ODM Embarcado UDF - Integração ILMT

Solução completa para execução de regras ODM no AWS Glue com rastreamento automático de licenças via ILMT.

## 📁 Estrutura

```
odm-embarcado-udf1/
├── jars/
│   └── odm-embarcado-udf-1.0.0.jar    # JAR compilado (40MB)
├── scripts/
│   └── glue_job_credito_pj_com_ilmt.py # Script AWS Glue
├── docs/
│   └── ILMT_INTEGRATION_GUIDE.md       # Documentação completa
├── src/
│   └── br/com/itau/odm/embarcado/      # Código fonte Java
│       ├── S3MetricsHelper.java
│       ├── DecisionMetering.java
│       ├── DecisionMeteringReport.java
│       ├── GenericODMUDF.java
│       └── ...
├── pom.xml                              # Maven build
└── README.md                            # Este arquivo
```

## 🚀 Quick Start

### 1. Upload do JAR para S3

```bash
aws s3 cp jars/odm-embarcado-udf-1.0.0.jar \
  s3://bre-laboratorio/embarcado/jars/
```

### 2. Criar Glue Job

```bash
# Via AWS CLI
aws glue create-job \
  --name credito-pj-ilmt \
  --role AWSGlueServiceRole-YourRole \
  --command '{"Name":"glueetl","ScriptLocation":"s3://your-bucket/scripts/glue_job_credito_pj_com_ilmt.py"}' \
  --default-arguments '{
    "--extra-jars":"s3://bre-laboratorio/embarcado/jars/odm-embarcado-udf-1.0.0.jar"
  }'
```

### 3. Executar

```bash
aws glue start-job-run --job-name credito-pj-ilmt
```

### 4. Verificar Relatórios ILMT

```bash
aws s3 ls s3://bre-laboratorio/embarcado/ilmt-reports/ --recursive
```

## 📊 Funcionalidades

### ✅ Execução ODM
- UDF genérica para qualquer ruleset
- Pool de sessões otimizado
- Cache de rulesets (XU)
- Suporte a múltiplos parâmetros I/O

### ✅ ILMT Integration
- **Geração automática** de relatórios após processamento
- **2 formatos XML**:
  - `ilmt-report-{timestamp}.xml` (formato IBM oficial)
  - `custom-report-{timestamp}.xml` (métricas customizadas)
- **Particionamento temporal**: `yyyy/MM/dd/HH/`
- **Métricas calculadas**:
  - `MILLION_MONTHLY_DECISIONS` (>= 1M decisões)
  - `THOUSAND_MONTHLY_ARTIFACTS` (< 1M decisões)

### ✅ Performance
- Spark AQE (Adaptive Query Execution)
- Paralelismo dinâmico
- S3 multipart upload otimizado
- Análise de percentis (P50, P95, P99)

## 📄 Exemplo de Relatório ILMT

```xml
<SchemaVersion>2.1.1</SchemaVersion>
<SoftwareIdentity>
  <PersistentId>b1a07d4dc0364452aa6206bb6584061d</PersistentId>
  <Name>IBM Operational Decision Manager Server</Name>
</SoftwareIdentity>
<Metric logTime="2024-03-09T12:30:00.000-0300">
  <Type>MILLION_MONTHLY_DECISIONS</Type>
  <Value>4.999</Value>
  <Period>
    <StartTime>2024-03-09T12:00:00.000-0300</StartTime>
    <EndTime>2024-03-09T12:30:00.000-0300</EndTime>
  </Period>
</Metric>
```

## ⚙️ Configuração

### Script Python (glue_job_credito_pj_com_ilmt.py)

```python
# Configuração ILMT (linhas 525-527)
S3_ILMT_BUCKET = "bre-laboratorio"
S3_ILMT_PREFIX = "embarcado/ilmt-reports/"
S3_ILMT_REGION = "sa-east-1"
```

### Permissões IAM

```json
{
  "Effect": "Allow",
  "Action": ["s3:PutObject"],
  "Resource": ["arn:aws:s3:::bre-laboratorio/embarcado/ilmt-reports/*"]
}
```

## 🔧 Build (Opcional)

Se precisar recompilar o JAR:

```bash
# Instalar dependências no lib/
# Executar Maven
mvn clean package -DskipTests

# JAR gerado em: target/odm-embarcado-udf-1.0.0.jar
```

## 📚 Documentação

- **Guia Completo**: `docs/ILMT_INTEGRATION_GUIDE.md`
- **Código Fonte**: `src/br/com/itau/odm/embarcado/`
- **Build Config**: `pom.xml`

## 🐛 Troubleshooting

### Problema: Relatórios não aparecem no S3

**Verificar:**
1. Credenciais AWS configuradas
2. Permissões IAM no bucket
3. Logs CloudWatch do Glue Job
4. Procurar por `[S3-HELPER]` nos logs

### Problema: "Could not find S3MetricsHelper"

**Solução:**
- Confirmar que JAR está no `--extra-jars`
- Verificar que JAR contém a classe:
  ```bash
  jar -tf odm-embarcado-udf-1.0.0.jar | grep S3MetricsHelper
  ```

## 📈 Métricas de Performance

O script exibe automaticamente:
- Throughput (registros/segundo)
- Tempo médio por decisão
- Percentis P50, P95, P99
- Detecção de cold starts

## 🔐 Segurança

- Credenciais AWS via IAM Role (não hardcoded)
- JARs assinados (Maven Shade Plugin)
- Logs sem dados sensíveis

## 📦 Versão

- **Versão**: 1.0.0
- **ODM**: 8.12.0
- **Spark**: 3.3.0
- **AWS SDK**: 1.12.529

## 👥 Suporte

Para dúvidas ou problemas:
1. Consulte `docs/ILMT_INTEGRATION_GUIDE.md`
2. Verifique logs do CloudWatch
3. Abra issue no repositório

---

**Status**: ✅ Pronto para Produção  
**Última Atualização**: 2024-03-09