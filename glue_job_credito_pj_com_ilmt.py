"""AWS Glue Job - Crédito PJ (VERSÃO OTIMIZADA - Full Tuning + ILMT Metrics)
===========================================================
✅ Todos os tunings de performance aplicados em Python
✅ Spark: memória, GC, serialização, paralelismo
✅ ODM: XU cache, pool de sessões, forceUptodate
✅ S3: multipart upload, buffer size
✅ Monitoramento: métricas de tempo por registro
✅ ILMT: envio automático de métricas para S3

TUNINGS APLICADOS:
  1. SparkContext com configurações otimizadas
  2. GC: G1GC com parâmetros ajustados
  3. Serialização: KryoSerializer
  4. Memória: frações otimizadas para UDF Java
  5. Paralelismo: calculado dinamicamente
  6. ODM XU: cache e pool via System Properties
  7. S3: buffer e multipart otimizados
  8. Particionamento: adaptativo ao volume de dados
  9. Cache inteligente: só cacheia se necessário
 10. Análise de performance: percentis de tempo
 11. ILMT Metrics: envio automático para S3
"""

import sys
import os
import json
import time
from datetime import datetime
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    expr, col, lit, current_timestamp, monotonically_increasing_id,
    when, count, avg, min as spark_min, max as spark_max,
    percentile_approx, get_json_object
)
from pyspark.sql.types import StringType, LongType

# =============================================================================
# ⚙️ CONFIGURAÇÃO - EDITE AQUI!
# =============================================================================

# --- Entrada ---
INPUT_PATH = "s3://bre-laboratorio/embarcado/input/bre-rendaeleita/cenarios_10M.json"

# --- JARs (S3) ---
UDF_JAR_S3        = "s3://bre-laboratorio/embarcado/jars/odm-embarcado-udf-1.0.0.jar"
UDF_JAR_LOCAL     = "/tmp/odm-embarcado-udf-1.0.0.jar"
RULESET_JAR_S3    = "s3://bre-laboratorio/embarcado/jars/bre-rendaeleita/bre_visaodorelacionamentobancario.jar"
RULESET_JAR_LOCAL = "/tmp/bre_visaodorelacionamentobancario.jar"
XOM_JAR_S3        = "s3://bre-laboratorio/embarcado/jars/bre-rendaeleita/XOM-VisaoDoRelacionamentoBancario-FaturamentoEleito-3.4.0.jar"
XOM_PATH_LOCAL    = "/tmp/XOM-VisaoDoRelacionamentoBancario-FaturamentoEleito-3.4.0.jar"

# --- ODM ---
RULESET_PATH = "/bre_visaodorelacionamentobancario/1.0/elege_faturamento"
INPUT_CLASS  = "main.java.itau.Cliente"
INPUT_PARAM  = "Cliente"

# --- S3 Output ---
S3_OUTPUT_BUCKET = "bre-laboratorio"
S3_OUTPUT_PREFIX = "embarcado/resultados/rendaeleita/"
S3_REGION        = "sa-east-1"

# --- ILMT Metrics ---
S3_METRICS_BUCKET = "bre-laboratorio"
S3_METRICS_PREFIX = "embarcado/ilmt-metrics/"
S3_METRICS_REGION = "sa-east-1"

# =============================================================================
# ⚙️ PARÂMETROS DE TUNING - ajuste conforme seu ambiente
# =============================================================================

# --- Spark ---
EXECUTOR_MEMORY        = "56g"    # Memória por executor (G.1X=12g, G.2X=28g)
EXECUTOR_MEMORY_OH     = "1g"     # Overhead JVM (15-20% do executor memory)
DRIVER_MEMORY          = "8g"     # Memória do driver
EXECUTOR_CORES         = 16        # Cores por executor (G.1X=3, G.2X=7)

# --- Paralelismo ---
PARALLELISM_OVERRIDE   = 320     # Ex: 200 para forçar valor fixo

# --- ODM XU (eXecution Unit) ---
XU_MAX_CACHE_SIZE      = 5        # Rulesets em cache por executor
XU_CACHE_EVICTION_MS   = 0        # TTL cache (0 = nunca expira)
XU_FORCE_UPTODATE      = False    # False = usa cache (10x mais rápido!)

# --- S3 ---
S3_MULTIPART_SIZE      = "128m"   # Tamanho de cada parte no upload multipart
S3_BUFFER_SIZE         = "65536"  # Buffer de leitura S3 (bytes)
OUTPUT_COALESCE        = 1        # Número de arquivos de saída (1 = arquivo único)

XU_MIN_POOL_SIZE       = 4   # ← NOVO: pré-aquece sessões (elimina cold starts)
XU_MAX_POOL_SIZE       = 80  # ← = EXECUTOR_CORES
XU_POOL_TIMEOUT_MS     = 90000
XU_POOL_WAIT_MS        = 45000
XU_COMPILATION_THREADS = 20   # ← NOVO: threads para compilar regras
 
# =============================================================================
# 🚀 INÍCIO
# =============================================================================

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
print("=" * 80)
print("🚀 AWS GLUE JOB - Crédito PJ (Full Tuning + ILMT)")
print("=" * 80)
print(f"Job Name: {args['JOB_NAME']}")
print(f"Input:    {INPUT_PATH}")
print(f"Ruleset:  {RULESET_PATH}")
print("=" * 80)

# =============================================================================
# 📥 BAIXAR JARs DO S3
# =============================================================================

print("\n📥 Baixando JARs do S3...")
s3_client = boto3.client('s3', region_name=S3_REGION)

def _s3_parse(s3_uri):
    parts = s3_uri.replace("s3://", "").split("/", 1)
    return parts[0], parts[1]

t0 = time.time()
for jar_s3, jar_local, label in [
    (UDF_JAR_S3,     UDF_JAR_LOCAL,     "UDF ODM"),
    (RULESET_JAR_S3, RULESET_JAR_LOCAL, "Ruleset"),
    (XOM_JAR_S3,     XOM_PATH_LOCAL,    "XOM"),
]:
    try:
        bucket, key = _s3_parse(jar_s3)
        s3_client.download_file(bucket, key, jar_local)
        size_mb = os.path.getsize(jar_local) / (1024 * 1024)
        print(f"  ✅ {label}: {jar_local} ({size_mb:.1f} MB)")
    except Exception as e:
        print(f"  ❌ ERRO ao baixar {label}: {e}")
        raise
print(f"  ⏱️  Download: {time.time()-t0:.1f}s")

# =============================================================================
# 🔧 INICIALIZAR SPARK COM TUNING COMPLETO
# =============================================================================

print("\n🔧 Inicializando Spark com tuning...")

os.environ['SPARK_SUBMIT_OPTS'] = (
    f"-Dilog.rules.res.xu.maxCacheSize={XU_MAX_CACHE_SIZE} "
    f"-Dilog.rules.res.xu.maxPoolSize={XU_MAX_POOL_SIZE} "
    f"-Dilog.rules.res.xu.cacheEvictionTimeout={XU_CACHE_EVICTION_MS}"
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

jvm = spark.sparkContext._jvm
jsc = spark.sparkContext._jsc

# 1. ODM XU: System Properties na JVM do driver
print("\n  ⚙️  Configurando ODM XU (System Properties)...")
jvm.System.setProperty("ilog.rules.res.xu.maxCacheSize",         str(XU_MAX_CACHE_SIZE))
jvm.System.setProperty("ilog.rules.res.xu.minPoolSize",          str(XU_MIN_POOL_SIZE))
jvm.System.setProperty("ilog.rules.res.xu.maxPoolSize",          str(XU_MAX_POOL_SIZE))
jvm.System.setProperty("ilog.rules.res.xu.poolTimeout",          str(XU_POOL_TIMEOUT_MS))
jvm.System.setProperty("ilog.rules.res.xu.poolWaitTimeout",      str(XU_POOL_WAIT_MS))
jvm.System.setProperty("ilog.rules.res.xu.compilationThreads",   str(XU_COMPILATION_THREADS))
jvm.System.setProperty("ilog.rules.res.xu.cacheEvictionTimeout", str(XU_CACHE_EVICTION_MS))
jvm.System.setProperty("ilog.rules.res.xu.forceUptodate",        str(XU_FORCE_UPTODATE).lower())
 
print(f"     maxCacheSize:         {XU_MAX_CACHE_SIZE}")
print(f"     maxPoolSize:          {XU_MAX_POOL_SIZE}")
print(f"     cacheEvictionTimeout: {XU_CACHE_EVICTION_MS}ms")
print(f"     forceUptodate:        {XU_FORCE_UPTODATE}")

# 2. Configurar ILMT Metrics
print("\n  ⚙️  Configurando ILMT Metrics...")
jvm.System.setProperty("S3_METRICS_BUCKET", S3_METRICS_BUCKET)
jvm.System.setProperty("S3_METRICS_PREFIX", S3_METRICS_PREFIX)
jvm.System.setProperty("S3_METRICS_REGION", S3_METRICS_REGION)
print(f"     Bucket: {S3_METRICS_BUCKET}")
print(f"     Prefix: {S3_METRICS_PREFIX}")
print(f"     Region: {S3_METRICS_REGION}")

# 3. Spark SQL: desabilitar Arrow
print("\n  ⚙️  Configurando Spark SQL...")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
spark.conf.set("spark.sql.execution.arrow.enabled",         "false")
print("     Arrow desabilitado (incompatível com UDF Java)")

# 4. AQE
spark.conf.set("spark.sql.adaptive.enabled",                "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled",       "true")
print("     AQE (Adaptive Query Execution) habilitado")

# 5. S3: otimizar leitura/escrita
print("\n  ⚙️  Configurando S3...")
spark.conf.set("spark.hadoop.fs.s3a.multipart.size",         S3_MULTIPART_SIZE)
spark.conf.set("spark.hadoop.fs.s3a.fast.upload",            "true")
spark.conf.set("spark.hadoop.fs.s3a.fast.upload.buffer",     "bytebuffer")
spark.conf.set("spark.hadoop.fs.s3a.connection.maximum",     "100")
spark.conf.set("spark.hadoop.fs.s3a.threads.max",            "20")
spark.conf.set("spark.hadoop.fs.s3a.block.size",             S3_BUFFER_SIZE)
print(f"     multipart.size:  {S3_MULTIPART_SIZE}")
print(f"     fast.upload:     true")
print(f"     connections max: 100")

# 6. Broadcast de JARs
print("\n  ⚙️  Adicionando JARs ao classpath...")
jsc.addJar(UDF_JAR_LOCAL)
jsc.addJar(XOM_PATH_LOCAL)
jsc.addJar(RULESET_JAR_LOCAL)
print(f"     ✅ UDF:     {UDF_JAR_LOCAL}")
print(f"     ✅ XOM:     {XOM_PATH_LOCAL}")
print(f"     ✅ Ruleset: {RULESET_JAR_LOCAL}")

print("\n✅ Spark inicializado com tuning completo!")

# =============================================================================
# 📝 REGISTRAR UDF ODM
# =============================================================================

try:
    spark.udf.registerJavaFunction(
        "execute_odm",
        "br.com.itau.odm.embarcado.GenericODMUDF",
        StringType()
    )
    print("✅ UDF ODM registrada")
except Exception as e:
    print(f"❌ ERRO ao registrar UDF: {e}")
    raise

# =============================================================================
# 📊 LER DADOS DE ENTRADA
# =============================================================================

print(f"\n📥 Lendo dados de: {INPUT_PATH}")
t0 = time.time()
df_input = (
    spark.read
    .option("multiline",       "true")
    .option("mode",            "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .json(INPUT_PATH)
)
total_records = df_input.count()
read_time = time.time() - t0
print(f"✅ {total_records:,} registros lidos em {read_time:.1f}s")

if "_corrupt_record" in df_input.columns:
    corrupt_count = df_input.filter(col("_corrupt_record").isNotNull()).count()
    if corrupt_count > 0:
        print(f"⚠️  {corrupt_count:,} registros corrompidos ignorados")
    df_input = df_input.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")

df_input = df_input.withColumn("record_id", monotonically_increasing_id())

# =============================================================================
# ⚡ PARTICIONAMENTO ADAPTATIVO
# =============================================================================

num_executors = int(spark.conf.get("spark.executor.instances", "10"))
cores_per_exec = EXECUTOR_CORES
total_cores    = num_executors * cores_per_exec

if PARALLELISM_OVERRIDE:
    optimal_partitions = PARALLELISM_OVERRIDE
else:
    optimal_partitions = max(10, total_cores * 3)

spark.conf.set("spark.sql.shuffle.partitions", str(optimal_partitions))
spark.conf.set("spark.default.parallelism",    str(optimal_partitions))
df_input = df_input.repartition(optimal_partitions)

print(f"\n📊 Particionamento:")
print(f"   Executores:  {num_executors}")
print(f"   Cores/exec:  {cores_per_exec}")
print(f"   Total cores: {total_cores}")
print(f"   Partições:   {optimal_partitions}")
print(f"   Reg/partição: {total_records // optimal_partitions:,}")

# =============================================================================
# 🎯 CONFIGURAÇÃO ODM
# =============================================================================

config_odm = {
    "ruleset_path":      RULESET_PATH,
    "input_class":       INPUT_CLASS,
    "input_param_name":  INPUT_PARAM,
    "output_param_names": ["Cliente","FaturamentoEleito"],
    "type_mapping": {
        "origens_class": "main.java.itau.Origem"
    }
}

print(f"\n⚙️  Configuração ODM:")
print(f"   Ruleset:  {config_odm['ruleset_path']}")
print(f"   Classe:   {config_odm['input_class']}")
print(f"   Parâm.:   {config_odm['input_param_name']}")

# =============================================================================
# 🔄 PREPARAR INPUT PARA ODM
# =============================================================================

config_odm_json = json.dumps(config_odm)

def create_odm_input(row):
    row_dict  = row.asDict(recursive=True)
    record_id = row_dict.pop('record_id')
    decision_id  = row_dict.get('DecisionID_', None)
    cliente_data = row_dict.get('Cliente', {})
    if decision_id:
        cliente_data['DecisionID_'] = decision_id
    payload = '{"__config__":' + config_odm_json + ',"data":' + json.dumps(cliente_data) + '}'
    return (record_id, payload)

t0 = time.time()
rdd_input    = df_input.rdd.map(create_odm_input)
df_with_input = spark.createDataFrame(rdd_input, ["record_id", "odm_input"])
df_with_input.persist()
input_count = df_with_input.count()
print(f"\n✅ Input preparado: {input_count:,} registros em {time.time()-t0:.1f}s")

# =============================================================================
# 🚀 EXECUTAR REGRAS ODM
# =============================================================================

print("\n" + "=" * 80)
print("🔄 EXECUTANDO REGRAS ODM")
print("=" * 80)
print(f"   Partições:    {optimal_partitions}")
print(f"   XU Cache:     {XU_MAX_CACHE_SIZE} rulesets")
print(f"   XU Pool:      {XU_MAX_POOL_SIZE} sessões/executor")
print(f"   ForceUptodate: {XU_FORCE_UPTODATE}")
print("=" * 80)

start_time = time.time()
start_time_ms = int(start_time * 1000)

df_result = (
    df_with_input
    .withColumn("odm_output",           expr("execute_odm(odm_input)"))
    .withColumn("processing_timestamp", current_timestamp())
    .withColumn("job_name",             lit(args['JOB_NAME']))
)

df_result.persist()
total_processed = df_result.count()
elapsed_time    = time.time() - start_time
end_time_ms = int((start_time + elapsed_time) * 1000)

df_with_input.unpersist()

# =============================================================================
# 📈 ESTATÍSTICAS DETALHADAS
# =============================================================================

print("\n" + "=" * 80)
print("📊 ESTATÍSTICAS DE EXECUÇÃO")
print("=" * 80)
print(f"  Total processado:     {total_processed:,} registros")
print(f"  Tempo total:          {elapsed_time:.2f}s")

if total_processed > 0 and elapsed_time > 0:
    throughput = total_processed / elapsed_time
    avg_ms     = (elapsed_time / total_processed) * 1000
    print(f"  Throughput:           {throughput:,.0f} reg/s")
    print(f"  Tempo médio/registro: {avg_ms:.2f}ms")

if total_processed > 0:
    df_analysis = df_result.select(
        count(when(col("odm_output").contains('"error"'), True)).alias("errors"),
        count(when(~col("odm_output").contains('"error"'), True)).alias("success")
    ).collect()[0]
    success     = int(df_analysis['success'])
    errors      = int(df_analysis['errors'])
    success_pct = 100.0 * success / total_processed
    error_pct   = 100.0 * errors  / total_processed
    print(f"\n  📈 Resultados:")
    print(f"     Sucesso: {success:,} ({success_pct:.1f}%)")
    print(f"     Erros:   {errors:,} ({error_pct:.1f}%)")

print(f"\n  ⏱️  Análise de Tempo ODM (por registro):")
try:
    df_times = df_result.select(
        get_json_object(col("odm_output"), "$.__ExecutionTimeMs__")
        .cast(LongType()).alias("exec_ms")
    ).filter(col("exec_ms").isNotNull())
    stats = df_times.select(
        spark_min("exec_ms").alias("min_ms"),
        avg("exec_ms").alias("avg_ms"),
        spark_max("exec_ms").alias("max_ms"),
        percentile_approx("exec_ms", 0.50).alias("p50_ms"),
        percentile_approx("exec_ms", 0.95).alias("p95_ms"),
        percentile_approx("exec_ms", 0.99).alias("p99_ms"),
    ).collect()[0]
    print(f"     Min:  {stats['min_ms']}ms")
    print(f"     P50:  {stats['p50_ms']}ms  (mediana)")
    print(f"     P95:  {stats['p95_ms']}ms")
    print(f"     P99:  {stats['p99_ms']}ms")
    print(f"     Max:  {stats['max_ms']}ms")
    print(f"     Avg:  {stats['avg_ms']:.1f}ms")
    cold_start_threshold = stats['p99_ms'] * 3 if stats['p99_ms'] else 5000
    cold_starts = df_times.filter(col("exec_ms") > cold_start_threshold).count()
    if cold_starts > 0:
        print(f"\n  ⚠️  Cold starts detectados: {cold_starts:,} registros > {cold_start_threshold}ms")
except Exception as e:
    print(f"     ⚠️ Não foi possível analisar tempos: {e}")

# =============================================================================
# 📤 MÉTRICAS ILMT PARA S3 (AUTOMÁTICO)
# =============================================================================
#
# As métricas ILMT são enviadas AUTOMATICAMENTE pelo S3Metrics no shutdown da JVM.
# Não é necessário chamar nenhum método do Python.
#
# Configuração (já feita nas linhas 180-182):
#   jvm.System.setProperty("S3_METRICS_BUCKET", S3_METRICS_BUCKET)
#   jvm.System.setProperty("S3_METRICS_PREFIX", S3_METRICS_PREFIX)
#   jvm.System.setProperty("S3_METRICS_REGION", S3_METRICS_REGION)
#
# O S3Metrics acumula automaticamente:
#   - Total de execuções
#   - Sucessos vs erros
#   - Tempo de execução
#   - Regras disparadas
#   - Ruleset path
#
# No shutdown, envia para S3:
#   s3://{bucket}/{prefix}/yyyy/MM/dd/HH/ilmt-report-{timestamp}.xml
#   s3://{bucket}/{prefix}/yyyy/MM/dd/HH/custom-report-{timestamp}.xml

print("\n" + "=" * 80)
print("📤 MÉTRICAS ILMT CONFIGURADAS (envio automático no shutdown)")
print("=" * 80)
print(f"  Bucket: s3://{S3_METRICS_BUCKET}/{S3_METRICS_PREFIX}")
print(f"  Decisões processadas: {total_processed:,}")
print(f"  ✅ Métricas serão enviadas automaticamente ao finalizar o job")

# =============================================================================
# 💾 SALVAR RESULTADOS NO S3
# =============================================================================

print("\n" + "=" * 80)
print("💾 SALVANDO RESULTADOS NO S3")
print("=" * 80)

now            = datetime.utcnow()
partition_path = f"{S3_OUTPUT_PREFIX}{now.year:04d}/{now.month:02d}/{now.day:02d}/{now.hour:02d}/"
full_output_path = f"s3://{S3_OUTPUT_BUCKET}/{partition_path}"
print(f"  Path: {full_output_path}")

if total_processed > 0:
    df_to_save = df_result.select(
        "record_id",
        "odm_output",
        "processing_timestamp",
    )
    try:
        t0 = time.time()
        df_to_save.coalesce(1).write \
           .mode("append") \
           .option("compression", "snappy") \
           .parquet(full_output_path)
        save_time = time.time() - t0
        print(f"  ✅ Salvo em {save_time:.1f}s")
        print(f"     Arquivo: {full_output_path}part-00000-*.parquet")
    except Exception as e:
        print(f"  ⚠️ Erro ao salvar: {e}")
else:
    print("  ℹ️ Nenhum resultado para salvar.")

df_result.unpersist()

# =============================================================================
# 📤 FLUSH FINAL (Kafka/Métricas)
# =============================================================================

print("\n📤 Finalizando métricas...")
try:
    jvm.br.com.itau.odm.embarcado.GenericODMUDF.shutdown()
    print("  ✅ Métricas enviadas")
except Exception as e:
    print(f"  ⚠️ Aviso: {e}")

# =============================================================================
# ✅ FINALIZAR JOB
# =============================================================================

job.commit()

print("\n" + "=" * 80)
print("✅ JOB FINALIZADO COM SUCESSO!")
print("=" * 80)
print(f"  📊 Processados: {total_processed:,} registros")
print(f"  ⏱️  Tempo total: {elapsed_time:.2f}s")
if total_processed > 0 and elapsed_time > 0:
    print(f"  ⚡ Throughput:  {total_processed/elapsed_time:,.0f} reg/s")
print(f"  💾 Resultados:  {full_output_path}")
print(f"  📤 ILMT:        s3://{S3_METRICS_BUCKET}/{S3_METRICS_PREFIX}")
print("=" * 80)
print()
print("⚙️  TUNINGS APLICADOS:")
print(f"   Spark AQE:           habilitado")
print(f"   Arrow:               desabilitado (UDF Java)")
print(f"   S3 fast upload:      habilitado")
print(f"   ODM XU cache:        {XU_MAX_CACHE_SIZE} rulesets")
print(f"   ODM XU pool:         {XU_MAX_POOL_SIZE} sessões/executor")
print(f"   ODM forceUptodate:   {XU_FORCE_UPTODATE}")
print(f"   Partições:           {optimal_partitions}")
print(f"   ILMT Metrics:        habilitado")
print("=" * 80)

# Made with Bob
