# Solução: "[S3-METRICS] Bucket não configurado - S3 desabilitado"

## Problema
Você configurou os job parameters no AWS Glue:
- `--S3_METRICS_BUCKET` = `bre-laboratorio`
- `--S3_METRICS_PREFIX` = `embarcado/metricas`
- `--S3_METRICS_REGION` = `sa-east-1`

Mas o código Java não está conseguindo ler esses parâmetros.

## Causa Raiz
No AWS Glue, os job parameters com `--` ficam disponíveis apenas no contexto Python. O código Java não tem acesso direto a eles.

## Solução

### Opção 1: Passar via Script Python (RECOMENDADO)

Adicione este código no **INÍCIO** do seu script Python do Glue, **ANTES** de criar o SparkContext:

```python
import sys
import os
from awsglue.utils import getResolvedOptions

# Obter parâmetros do Glue Job
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'S3_METRICS_BUCKET',
    'S3_METRICS_PREFIX', 
    'S3_METRICS_REGION'
])

# Configurar variáveis de ambiente para Java
os.environ['S3_METRICS_BUCKET'] = args['S3_METRICS_BUCKET']
os.environ['S3_METRICS_PREFIX'] = args['S3_METRICS_PREFIX']
os.environ['S3_METRICS_REGION'] = args['S3_METRICS_REGION']

# Agora crie o SparkContext
from pyspark.context import SparkContext
from awsglue.context import GlueContext

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Configurar System Properties do Java (garantia adicional)
spark._jvm.System.setProperty("S3_METRICS_BUCKET", args['S3_METRICS_BUCKET'])
spark._jvm.System.setProperty("S3_METRICS_PREFIX", args['S3_METRICS_PREFIX'])
spark._jvm.System.setProperty("S3_METRICS_REGION", args['S3_METRICS_REGION'])

print(f"[GLUE] S3 Metrics configurado: bucket={args['S3_METRICS_BUCKET']}, prefix={args['S3_METRICS_PREFIX']}, region={args['S3_METRICS_REGION']}")
```

### Opção 2: Usar Spark Configuration

Adicione no seu script Python, logo após criar o SparkContext:

```python
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
import sys

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_METRICS_BUCKET', 'S3_METRICS_PREFIX', 'S3_METRICS_REGION'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Configurar via Spark
spark.conf.set("spark.executorEnv.S3_METRICS_BUCKET", args['S3_METRICS_BUCKET'])
spark.conf.set("spark.executorEnv.S3_METRICS_PREFIX", args['S3_METRICS_PREFIX'])
spark.conf.set("spark.executorEnv.S3_METRICS_REGION", args['S3_METRICS_REGION'])

# E também via System Properties
spark._jvm.System.setProperty("S3_METRICS_BUCKET", args['S3_METRICS_BUCKET'])
spark._jvm.System.setProperty("S3_METRICS_PREFIX", args['S3_METRICS_PREFIX'])
spark._jvm.System.setProperty("S3_METRICS_REGION", args['S3_METRICS_REGION'])
```

### Opção 3: Hardcode Temporário (para teste)

Se você quiser testar rapidamente, adicione no início do script Python:

```python
import os

# Configuração hardcoded para teste
os.environ['S3_METRICS_BUCKET'] = 'bre-laboratorio'
os.environ['S3_METRICS_PREFIX'] = 'embarcado/metricas/'
os.environ['S3_METRICS_REGION'] = 'sa-east-1'
```

## Verificação

Após aplicar a solução, você deve ver nos logs do CloudWatch:

```
[GLUE] S3 Metrics configurado: bucket=bre-laboratorio, prefix=embarcado/metricas/, region=sa-east-1
[S3-METRICS] Inicializado: bucket=bre-laboratorio, prefix=embarcado/metricas/, region=sa-east-1
```

E no final da execução:

```
[S3-METRICS] Relatórios ILMT enviados para S3 (decisions=XXX)
[S3-METRICS] Arquivo enviado: s3://bre-laboratorio/embarcado/metricas/2026/03/05/15/ilmt-report-XXXXX.xml
```

## Exemplo Completo de Script Glue

Veja o arquivo `GLUE_SCRIPT_EXEMPLO.py` para um exemplo completo.

## Permissões IAM Necessárias

Certifique-se de que o IAM Role do Glue Job tem permissões para escrever no bucket:

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
      "Resource": "arn:aws:s3:::bre-laboratorio/embarcado/metricas/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket"
      ],
      "Resource": "arn:aws:s3:::bre-laboratorio"
    }
  ]
}
```

## Troubleshooting Adicional

### Se ainda não funcionar:

1. **Verifique os logs do CloudWatch** para ver se as variáveis estão sendo setadas
2. **Teste com valores hardcoded** primeiro (Opção 3)
3. **Verifique as permissões IAM** do role do Glue
4. **Confirme que o bucket existe** e está na região correta

### Logs úteis para debug:

```python
# Adicione no seu script Python para debug
print(f"Job parameters: {args}")
print(f"Environment vars: S3_METRICS_BUCKET={os.environ.get('S3_METRICS_BUCKET')}")
print(f"Java System Property: {spark._jvm.System.getProperty('S3_METRICS_BUCKET')}")
```

## Próximos Passos

Após aplicar a solução:
1. Execute o job novamente
2. Verifique os logs do CloudWatch
3. Confirme que os arquivos XML foram criados no S3
4. Verifique o conteúdo dos arquivos XML