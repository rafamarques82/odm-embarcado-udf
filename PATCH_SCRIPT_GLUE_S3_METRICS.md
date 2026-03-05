# Patch: Adicionar S3 Metrics ao seu script Glue

## Localização da Alteração

Adicione o código abaixo **LOGO APÓS** a linha:

```python
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
```

## Código a Adicionar

```python
# =============================================================================
# 📊 CONFIGURAR S3 METRICS (adicionar após getResolvedOptions)
# =============================================================================

# Obter parâmetros S3 do Glue Job (se configurados)
try:
    s3_metrics_args = getResolvedOptions(sys.argv, [
        'S3_METRICS_BUCKET',
        'S3_METRICS_PREFIX',
        'S3_METRICS_REGION'
    ])
    
    # Configurar variáveis de ambiente para Java
    os.environ['S3_METRICS_BUCKET'] = s3_metrics_args['S3_METRICS_BUCKET']
    os.environ['S3_METRICS_PREFIX'] = s3_metrics_args['S3_METRICS_PREFIX']
    os.environ['S3_METRICS_REGION'] = s3_metrics_args['S3_METRICS_REGION']
    
    print("=" * 80)
    print("📊 S3 METRICS CONFIGURADO")
    print("=" * 80)
    print(f"  Bucket: {s3_metrics_args['S3_METRICS_BUCKET']}")
    print(f"  Prefix: {s3_metrics_args['S3_METRICS_PREFIX']}")
    print(f"  Region: {s3_metrics_args['S3_METRICS_REGION']}")
    print("=" * 80)
    
except Exception as e:
    print(f"⚠️  S3 Metrics não configurado (opcional): {e}")
    print("   Para habilitar, adicione os job parameters:")
    print("   --S3_METRICS_BUCKET, --S3_METRICS_PREFIX, --S3_METRICS_REGION")
```

## Alteração Adicional

Depois de criar o `spark` (após a linha `spark = glueContext.spark_session`), adicione:

```python
# Configurar System Properties do Java para S3 Metrics
if 'S3_METRICS_BUCKET' in os.environ:
    jvm = spark.sparkContext._jvm
    jvm.System.setProperty("S3_METRICS_BUCKET", os.environ['S3_METRICS_BUCKET'])
    jvm.System.setProperty("S3_METRICS_PREFIX", os.environ['S3_METRICS_PREFIX'])
    jvm.System.setProperty("S3_METRICS_REGION", os.environ['S3_METRICS_REGION'])
    print("  ✅ S3 Metrics: System Properties configuradas na JVM")
```

## Script Completo com as Alterações

Veja o exemplo abaixo mostrando onde inserir o código:

```python
# ... seu código existente ...

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# ========== ADICIONAR AQUI - INÍCIO ==========
# Configurar S3 Metrics
try:
    s3_metrics_args = getResolvedOptions(sys.argv, [
        'S3_METRICS_BUCKET',
        'S3_METRICS_PREFIX',
        'S3_METRICS_REGION'
    ])
    
    os.environ['S3_METRICS_BUCKET'] = s3_metrics_args['S3_METRICS_BUCKET']
    os.environ['S3_METRICS_PREFIX'] = s3_metrics_args['S3_METRICS_PREFIX']
    os.environ['S3_METRICS_REGION'] = s3_metrics_args['S3_METRICS_REGION']
    
    print("=" * 80)
    print("📊 S3 METRICS CONFIGURADO")
    print("=" * 80)
    print(f"  Bucket: {s3_metrics_args['S3_METRICS_BUCKET']}")
    print(f"  Prefix: {s3_metrics_args['S3_METRICS_PREFIX']}")
    print(f"  Region: {s3_metrics_args['S3_METRICS_REGION']}")
    print("=" * 80)
    
except Exception as e:
    print(f"⚠️  S3 Metrics não configurado: {e}")
# ========== ADICIONAR AQUI - FIM ==========

print("=" * 80)
print("🚀 AWS GLUE JOB - Crédito PJ (Full Tuning)")
# ... resto do código ...

# Depois de criar o spark:
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# ========== ADICIONAR AQUI - INÍCIO ==========
# Configurar System Properties do Java para S3 Metrics
if 'S3_METRICS_BUCKET' in os.environ:
    jvm = spark.sparkContext._jvm
    jvm.System.setProperty("S3_METRICS_BUCKET", os.environ['S3_METRICS_BUCKET'])
    jvm.System.setProperty("S3_METRICS_PREFIX", os.environ['S3_METRICS_PREFIX'])
    jvm.System.setProperty("S3_METRICS_REGION", os.environ['S3_METRICS_REGION'])
    print("  ✅ S3 Metrics: System Properties configuradas na JVM")
# ========== ADICIONAR AQUI - FIM ==========

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ... resto do código continua igual ...
```

## Verificação

Após aplicar as alterações e executar o job, você deve ver nos logs:

```
================================================================================
📊 S3 METRICS CONFIGURADO
================================================================================
  Bucket: bre-laboratorio
  Prefix: embarcado/metricas/
  Region: sa-east-1
================================================================================
```

E mais adiante:

```
  ✅ S3 Metrics: System Properties configuradas na JVM
```

E no final da execução:

```
[S3-METRICS] Inicializado: bucket=bre-laboratorio, prefix=embarcado/metricas/, region=sa-east-1
[S3-METRICS] Relatórios ILMT enviados para S3 (decisions=XXX)
```

## Resumo das Alterações

1. **Linha ~95** (após `args = getResolvedOptions`): Adicionar configuração S3 Metrics
2. **Linha ~150** (após `spark = glueContext.spark_session`): Adicionar System Properties

Essas são as ÚNICAS alterações necessárias no seu script!