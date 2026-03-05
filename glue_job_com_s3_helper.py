import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import StringType
from datetime import datetime

# Obter parâmetros do job
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'JAR_PATH',
    'RULESET_PATH',
    'INPUT_PARAM',
    'OUTPUT_PARAM',
    'S3_METRICS_BUCKET',
    'S3_METRICS_PREFIX',
    'S3_METRICS_REGION'
])

# Inicializar contextos
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configurar System Properties para S3Metrics
spark.sparkContext._jvm.System.setProperty("S3_METRICS_BUCKET", args['S3_METRICS_BUCKET'])
spark.sparkContext._jvm.System.setProperty("S3_METRICS_PREFIX", args.get('S3_METRICS_PREFIX', 'odm-metrics/'))
spark.sparkContext._jvm.System.setProperty("S3_METRICS_REGION", args.get('S3_METRICS_REGION', 'us-east-1'))

print(f"[GLUE] Configuração S3 Metrics:")
print(f"  Bucket: {args['S3_METRICS_BUCKET']}")
print(f"  Prefix: {args.get('S3_METRICS_PREFIX', 'odm-metrics/')}")
print(f"  Region: {args.get('S3_METRICS_REGION', 'us-east-1')}")

# Adicionar JAR ao classpath
spark.sparkContext.addPyFile(args['JAR_PATH'])

# Registrar UDF Java
spark.udf.registerJavaFunction(
    "odm_execute",
    "br.com.itau.odm.embarcado.GenericODMUDF",
    StringType()
)

# Criar DataFrame de exemplo (100.000 registros)
print("[GLUE] Criando DataFrame de teste com 100.000 registros...")
data = [{"id": i, "valor": i * 100, "tipo": "PJ"} for i in range(1, 100001)]
df = spark.createDataFrame(data)

print(f"[GLUE] DataFrame criado com {df.count()} registros")

# Converter para JSON e aplicar UDF
print("[GLUE] Aplicando UDF ODM...")
df_with_json = df.selectExpr("to_json(struct(*)) as input_json")
df_result = df_with_json.selectExpr("odm_execute(input_json) as output_json")

# Coletar métricas durante o processamento
start_time = datetime.now()
start_time_ms = int(start_time.timestamp() * 1000)
print(f"[GLUE] Início do processamento: {start_time.isoformat()}")

# Forçar execução e contar resultados
result_count = df_result.count()
end_time = datetime.now()
end_time_ms = int(end_time.timestamp() * 1000)

print(f"[GLUE] Fim do processamento: {end_time.isoformat()}")
print(f"[GLUE] Total de registros processados: {result_count}")

# Calcular duração
duration_seconds = (end_time - start_time).total_seconds()
print(f"[GLUE] Duração total: {duration_seconds:.2f} segundos")

# Mostrar alguns resultados
print("[GLUE] Primeiros 5 resultados:")
df_result.show(5, truncate=False)

# ============================================
# ENVIAR MÉTRICAS ILMT PARA S3 VIA JAVA
# ============================================

print("\n[GLUE] Enviando métricas ILMT para S3 via S3MetricsHelper...")

try:
    # Obter referência para a classe Java S3MetricsHelper
    S3MetricsHelper = spark.sparkContext._jvm.br.com.itau.odm.embarcado.S3MetricsHelper
    
    # Chamar método estático Java para enviar métricas
    success = S3MetricsHelper.sendIlmtMetrics(
        args['S3_METRICS_BUCKET'],                          # bucketName
        args.get('S3_METRICS_PREFIX', 'odm-metrics/'),     # prefix
        args.get('S3_METRICS_REGION', 'us-east-1'),        # region
        args['RULESET_PATH'],                               # rulesetPath
        result_count,                                       # totalDecisions
        start_time_ms,                                      # startTimeMs
        end_time_ms                                         # endTimeMs
    )
    
    if success:
        print(f"\n[GLUE] ✅ Job concluído com sucesso!")
        print(f"  - Registros processados: {result_count}")
        print(f"  - Duração: {duration_seconds:.2f}s")
        print(f"  - Métricas ILMT enviadas para S3")
    else:
        print(f"\n[GLUE] ⚠️  Job concluído mas houve erro ao enviar métricas")
        
except Exception as e:
    print(f"\n[GLUE] ❌ ERRO ao enviar métricas via S3MetricsHelper: {str(e)}")
    import traceback
    traceback.print_exc()

job.commit()

# Made with Bob
