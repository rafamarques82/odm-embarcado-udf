# Configuração S3Metrics no AWS Glue

## Visão Geral
O S3Metrics envia automaticamente métricas de execução do ODM para o S3 em formato ILMT (IBM License Metric Tool).

## Métodos de Configuração

### Opção 1: Job Parameters do AWS Glue (Recomendado)

Configure os seguintes job parameters no AWS Glue Console:

#### Como configurar no AWS Glue Console:

1. Acesse o AWS Glue Console
2. Navegue até **Jobs** → Selecione seu job
3. Na aba **Job details**, role até **Job parameters**
4. Adicione os seguintes parâmetros:

| Key | Value | Exemplo |
|-----|-------|---------|
| `--S3_METRICS_BUCKET` | Nome do bucket S3 | `s3://bre-laboratorio/embarcado` |
| `--S3_METRICS_PREFIX` | Prefixo/caminho no bucket | `metricas/` |
| `--S3_METRICS_REGION` | Região AWS | `sa-east-1` |

**Importante:**
- Os parâmetros devem começar com `--` (dois hífens)
- O valor do bucket pode incluir `s3://` ou apenas o nome do bucket
- O código remove automaticamente o prefixo `s3://` se presente

**Exemplo da sua configuração atual:**
```
--S3_METRICS_BUCKET = s3://bre-laboratorio/embarcado
--S3_METRICS_PREFIX = metricas/
--S3_METRICS_REGION = sa-east-1
```

### Opção 2: Variáveis de Ambiente

Alternativamente, você pode usar variáveis de ambiente (útil para testes locais):

```bash
export S3_METRICS_BUCKET=bre-laboratorio/embarcado
export S3_METRICS_PREFIX=metricas/
export S3_METRICS_REGION=sa-east-1
```

### Opção 2: Variáveis de Ambiente no AWS Glue

No AWS Glue, você pode configurar variáveis de ambiente de duas formas:

#### A) Via Console AWS Glue (Recomendado)

1. Acesse o AWS Glue Console
2. Navegue até **Jobs** → Selecione seu job
3. Na aba **Job details**, role até **Advanced properties**
4. Expanda a seção **Environment variables**
5. Adicione as variáveis:

```
S3_METRICS_BUCKET=bre-laboratorio/embarcado
S3_METRICS_PREFIX=metricas/
S3_METRICS_REGION=sa-east-1
```

**Nota:** Ao usar variáveis de ambiente, NÃO use o prefixo `s3://` no bucket.

#### B) Via Script Python do Glue Job

Se você estiver usando um script Python no Glue, pode definir as variáveis antes de chamar a UDF:

```python
import os
import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# Configurar variáveis de ambiente
os.environ['S3_METRICS_BUCKET'] = 'bre-laboratorio/embarcado'
os.environ['S3_METRICS_PREFIX'] = 'metricas/'
os.environ['S3_METRICS_REGION'] = 'sa-east-1'

# Seu código Glue continua aqui...
sc = SparkContext()
glueContext = GlueContext(sc)
```

#### C) Via AWS CLI (para criar/atualizar job)

```bash
aws glue update-job \
  --job-name seu-job-odm \
  --job-update '{
    "Command": {
      "Name": "glueetl",
      "ScriptLocation": "s3://seu-bucket/scripts/seu-script.py"
    },
    "DefaultArguments": {
      "--enable-metrics": "",
      "--enable-continuous-cloudwatch-log": "true"
    },
    "ExecutionProperty": {
      "MaxConcurrentRuns": 1
    },
    "Environment": {
      "S3_METRICS_BUCKET": "bre-laboratorio/embarcado",
      "S3_METRICS_PREFIX": "metricas/",
      "S3_METRICS_REGION": "sa-east-1"
    }
  }'
```

**Nota:** A sintaxe exata pode variar dependendo da versão do AWS CLI.

#### D) Via CloudFormation/Terraform

**CloudFormation:**
```yaml
Resources:
  MyGlueJob:
    Type: AWS::Glue::Job
    Properties:
      Name: odm-embarcado-job
      Role: !GetAtt GlueJobRole.Arn
      Command:
        Name: glueetl
        ScriptLocation: s3://bucket/script.py
      DefaultArguments:
        "--job-language": "python"
        "--S3_METRICS_BUCKET": "bre-laboratorio/embarcado"
        "--S3_METRICS_PREFIX": "metricas/"
        "--S3_METRICS_REGION": "sa-east-1"
```

**Terraform:**
```hcl
resource "aws_glue_job" "odm_job" {
  name     = "odm-embarcado-job"
  role_arn = aws_iam_role.glue_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://bucket/script.py"
  }

  default_arguments = {
    "--job-language"        = "python"
    "--S3_METRICS_BUCKET"   = "bre-laboratorio/embarcado"
    "--S3_METRICS_PREFIX"   = "metricas/"
    "--S3_METRICS_REGION"   = "sa-east-1"
  }
}
```

### Opção 3: Arquivo de Configuração

Crie um arquivo `s3-config.properties` no classpath:

```properties
s3.metrics.bucket=nome-do-seu-bucket
s3.metrics.prefix=odm-metrics/
s3.metrics.region=us-east-1
```

Para usar no Glue:
1. Faça upload do arquivo para S3
2. Configure o caminho no job parameter: `--extra-files=s3://seu-bucket/s3-config.properties`

## Estrutura de Arquivos no S3

Os arquivos são organizados automaticamente por data:

```
s3://seu-bucket/odm-metrics/
├── 2026/
│   ├── 03/
│   │   ├── 04/
│   │   │   ├── 20/
│   │   │   │   ├── ilmt-report-1709582400000.xml
│   │   │   │   └── custom-report-1709582400000.xml
```

Formato: `{prefix}/{yyyy}/{MM}/{dd}/{HH}/{tipo}-{timestamp}.xml`

## Tipos de Relatórios

### 1. ILMT Report (ilmt-report-*.xml)
Formato compatível com IBM License Metric Tool:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<DecisionServiceMetering>
  <RulesetPath>/path/to/ruleset</RulesetPath>
  <StartTime>2026-03-04T17:00:00.000-0300</StartTime>
  <EndTime>2026-03-04T20:00:00.000-0300</EndTime>
  <TotalDecisions>1000</TotalDecisions>
  <ProductName>IBM Operational Decision Manager</ProductName>
  <ProductVersion>8.12.0</ProductVersion>
</DecisionServiceMetering>
```

### 2. Custom Report (custom-report-*.xml)
Relatório detalhado com estatísticas:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<ODMExecutionSummary>
  <RulesetPath>/path/to/ruleset</RulesetPath>
  <TimeWindow>
    <Start>2026-03-04T17:00:00.000-0300</Start>
    <End>2026-03-04T20:00:00.000-0300</End>
  </TimeWindow>
  <Executions>
    <Total>1000</Total>
    <Success>995</Success>
    <Errors>5</Errors>
  </Executions>
  <Performance>
    <TotalDurationMs>50000</TotalDurationMs>
    <AverageDurationMs>50</AverageDurationMs>
    <TotalRulesFired>5000</TotalRulesFired>
  </Performance>
</ODMExecutionSummary>
```

## Permissões IAM Necessárias

O IAM Role do Glue Job precisa das seguintes permissões:

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
      "Resource": "arn:aws:s3:::seu-bucket/odm-metrics/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket"
      ],
      "Resource": "arn:aws:s3:::seu-bucket"
    }
  ]
}
```

## Como Funciona

1. **Inicialização Automática**: O S3Metrics é inicializado automaticamente na primeira execução
2. **Acumulação de Métricas**: Cada execução ODM incrementa contadores thread-safe
3. **Envio no Shutdown**: No encerramento do job, envia relatório agregado para S3
4. **Particionamento**: Arquivos são organizados por data/hora automaticamente

## Uso no Código

O S3Metrics é chamado automaticamente pelo `GenericODMUDF`. Não requer código adicional.

```java
// Já integrado no GenericODMUDF
S3Metrics.recordExecution(rulesetPath, durationMs, rulesFired, success);
```

## Verificação

### Logs do Glue
Procure por estas mensagens nos logs:
```
[S3-METRICS] Inicializado: bucket=seu-bucket, prefix=odm-metrics/, region=us-east-1
[S3-METRICS] Arquivo enviado: s3://seu-bucket/odm-metrics/2026/03/04/20/ilmt-report-1709582400000.xml
[S3-METRICS] Relatórios ILMT enviados para S3 (decisions=1000)
```

### Verificar Arquivos no S3
```bash
aws s3 ls s3://seu-bucket/odm-metrics/ --recursive
```

## Troubleshooting

### S3 não configurado
```
[S3-METRICS] Bucket não configurado - S3 desabilitado
```
**Solução**: Configure a variável `S3_METRICS_BUCKET`

### Erro de permissão
```
[S3-METRICS] ERRO ao enviar resumo: Access Denied
```
**Solução**: Verifique as permissões IAM do role do Glue

### Nenhuma execução registrada
```
[S3-METRICS] Nenhuma execução registrada - resumo NÃO enviado
```
**Solução**: Verifique se o ODM está sendo executado corretamente

## Estatísticas

Para verificar estatísticas em tempo de execução:
```java
String stats = S3Metrics.getStats();
// Retorna: S3Metrics[sent=2, errors=0, executions=1000, ready=true]
```

## Exemplo Completo de Configuração no Glue

### Via Console:
1. **Job parameters**:
   - `--S3_METRICS_BUCKET` = `meu-bucket-odm-metrics`
   - `--S3_METRICS_PREFIX` = `producao/odm-metrics/`
   - `--S3_METRICS_REGION` = `us-east-1`

2. **IAM Role**: Anexar policy com permissões S3

3. **Executar o job**: Métricas serão enviadas automaticamente no shutdown

### Via AWS CLI:
```bash
aws glue update-job \
  --job-name meu-odm-job \
  --job-update '{
    "DefaultArguments": {
      "--S3_METRICS_BUCKET": "meu-bucket-odm-metrics",
      "--S3_METRICS_PREFIX": "producao/odm-metrics/",
      "--S3_METRICS_REGION": "us-east-1"
    }
  }'
```

## Desabilitar S3Metrics

Para desabilitar, simplesmente não configure a variável `S3_METRICS_BUCKET`. O sistema funcionará normalmente sem enviar métricas.

## Suporte

Para dúvidas ou problemas, verifique:
1. Logs do CloudWatch do Glue Job
2. Permissões IAM
3. Configuração das variáveis de ambiente
4. Conectividade com S3