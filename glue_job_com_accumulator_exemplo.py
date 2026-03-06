"""
Exemplo de uso do S3MetricsAccumulator para agregar métricas de todos os executors.

IMPORTANTE: Este é um exemplo de como usar. Você precisa adaptar seu script existente.
"""

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Inicializar Spark
spark = SparkSession.builder.appName("ODM-ILMT-Accumulator").getOrCreate()
sc = spark.sparkContext

# Adicionar JARs
sc.addJar("s3://bre-laboratorio/embarcado/jars/odm-embarcado-udf-1.0.0.jar")
sc.addJar("s3://bre-laboratorio/embarcado/jars/xom.jar")
sc.addJar("s3://bre-laboratorio/embarcado/jars/ruleset.jar")

# Registrar UDF
spark.udf.registerJavaFunction("execute_odm", "br.com.itau.odm.embarcado.GenericODMUDF", StringType())

# ===== CRIAR ACCUMULATOR =====
jvm = sc._jvm
AccumulatorClass = jvm.br.com.itau.odm.embarcado.S3MetricsAccumulator
metrics_accumulator = AccumulatorClass()

# Registrar no Spark
sc.register(metrics_accumulator, "ODM_Metrics")

# ===== BROADCAST DO ACCUMULATOR =====
# Importante: fazer broadcast para que todos os executors tenham acesso
accumulator_broadcast = sc.broadcast(metrics_accumulator)

# ===== LER DADOS =====
df = spark.read.json("s3://bre-laboratorio/embarcado/input/data.json")

# ===== EXECUTAR ODM COM ACCUMULATOR =====
# Opção 1: Modificar GenericODMUDF para aceitar accumulator (requer mudança no código Java)
# Opção 2: Usar uma UDF wrapper em Python (mais simples)

def execute_odm_with_metrics(input_json):
    """Wrapper que chama a UDF Java e registra métricas no accumulator"""
    import time
    start = time.time()
    
    # Chamar UDF Java
    result = spark.sql(f"SELECT execute_odm('{input_json}') as result").collect()[0].result
    
    duration_ms = int((time.time() - start) * 1000)
    
    # Registrar no accumulator
    acc = accumulator_broadcast.value
    acc.recordExecution(
        "/ruleset/path",  # ruleset path
        duration_ms,       # duration
        10,                # rules fired (você precisa extrair do result)
        True               # success
    )
    
    return result

# Registrar UDF Python
from pyspark.sql.types import StringType
spark.udf.register("execute_odm_tracked", execute_odm_with_metrics, StringType())

# Executar
df_result = df.withColumn("odm_output", expr("execute_odm_tracked(input_column)"))
df_result.count()  # Trigger execution

# ===== COLETAR MÉTRICAS AGREGADAS =====
final_metrics = metrics_accumulator.value()

print(f"Total execuções: {final_metrics.totalCount}")
print(f"Sucesso: {final_metrics.okCount}")
print(f"Erros: {final_metrics.errorCount}")
print(f"Duração total: {final_metrics.totalDurationMs}ms")
print(f"Média: {final_metrics.totalDurationMs / final_metrics.totalCount if final_metrics.totalCount > 0 else 0}ms")
print(f"Ruleset: {final_metrics.rulesetPath}")

# ===== ENVIAR PARA S3 =====
# Agora você tem as métricas agregadas de TODOS os executors
# Pode usar S3MetricsHelper para enviar um único XML com o total

S3MetricsHelper = jvm.br.com.itau.odm.embarcado.S3MetricsHelper
S3MetricsHelper.sendMetricsToS3(
    "bre-laboratorio",
    "embarcado/ilmt-metrics",
    "us-east-1",
    final_metrics.totalCount,
    final_metrics.okCount,
    final_metrics.errorCount,
    final_metrics.totalDurationMs,
    final_metrics.totalRulesFired,
    final_metrics.rulesetPath,
    final_metrics.startTimestampMs,
    final_metrics.endTimestampMs
)

print("✅ Métricas ILMT enviadas para S3 com sucesso!")

# Made with Bob
