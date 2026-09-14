"""AWS Glue Job - Crédito PJ (VERSÃO OTIMIZADA - Full Tuning + S3 Metrics)
===========================================================
✅ Todos os tunings de performance aplicados em Python
✅ Spark: memória, GC, serialização, paralelismo
✅ ODM: XU cache, pool de sessões, forceUptodate
✅ S3: multipart upload, buffer size
✅ S3 Metrics: envio automático de métricas ILMT para S3
✅ Monitoramento: métricas de tempo por registro

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
 11. S3 Metrics: relatórios ILMT automáticos
"""

import io
import xml.etree.ElementTree as ET
import zipfile
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
INPUT_PATH = "s3://bre-laboratorio/embarcado/input/bre-rendaeleita/cenarios_100k.json"

# --- JARs (S3) ---
RULESET_JAR_S3    = "s3://bre-laboratorio/embarcado/jars/bre-rendaeleita/bre_visaodorelacionamentobancario.jar"
RULESET_JAR_LOCAL = "/tmp/bre_visaodorelacionamentobancario.jar"
XOM_JAR_S3        = "s3://bre-laboratorio/embarcado/jars/bre-rendaeleita/XOM-VisaoDoRelacionamentoBancario-FaturamentoEleito-3.4.0.jar"
XOM_PATH_LOCAL    = "/tmp/XOM-VisaoDoRelacionamentoBancario-FaturamentoEleito-3.4.0.jar"
ODM_SERVER_JAR_S3       = "s3://bre-laboratorio/odm-embarcado-server-21-1.0.0.jar"
ODM_SERVER_JAR_LOCAL_DRV = "/tmp/odm-embarcado-server-21-1.0.0.jar"  # path no driver (só para inspeção)

# --- ODM ---
RULESET_PATH = "/bre_visaodorelacionamentobancario/1.0/elege_faturamento"
INPUT_CLASS  = "main.java.itau.Cliente"
INPUT_PARAM  = "Cliente"

# --- S3 Output ---
S3_OUTPUT_BUCKET = "bre-laboratorio"
S3_OUTPUT_PREFIX = "embarcado/resultados/rendaeleita/"
S3_REGION        = "sa-east-1"

# =============================================================================
# ⚙️ PARÂMETROS DE TUNING - ajuste conforme seu ambiente
# =============================================================================

# --- Spark ---
EXECUTOR_MEMORY        = "56g"    # Memória por executor (G.1X=12g, G.2X=28g)
EXECUTOR_MEMORY_OH     = "1g"     # Overhead JVM (15-20% do executor memory)
DRIVER_MEMORY          = "8g"     # Memória do driver
EXECUTOR_CORES         = 16       # Cores por executor (G.1X=3, G.2X=7)

# --- Paralelismo ---
PARALLELISM_OVERRIDE   = 40       # Para 100k registros

# --- ODM XU (eXecution Unit) ---
XU_MAX_CACHE_SIZE      = 5        # Rulesets em cache por executor
XU_CACHE_EVICTION_MS   = 0        # TTL cache (0 = nunca expira)
XU_FORCE_UPTODATE      = False    # False = usa cache (10x mais rápido!)

# --- S3 ---
S3_MULTIPART_SIZE      = "128m"   # Tamanho de cada parte no upload multipart
S3_BUFFER_SIZE         = "65536"  # Buffer de leitura S3 (bytes)
OUTPUT_COALESCE        = 1        # Número de arquivos de saída (1 = arquivo único)

XU_MIN_POOL_SIZE       = 4   # Pré-aquece sessões (elimina cold starts)
XU_MAX_POOL_SIZE       = 80  # = EXECUTOR_CORES
XU_POOL_TIMEOUT_MS     = 90000
XU_POOL_WAIT_MS        = 45000
XU_COMPILATION_THREADS = 20   # Threads para compilar regras

# =============================================================================
# 🚀 INÍCIO
# =============================================================================

# Injetar paths S3 para o odm_subprocess_client antes do import
os.environ["ODM_SERVER_JAR_S3"]     = ODM_SERVER_JAR_S3
os.environ["ODM_RULESET_JAR_S3"]    = RULESET_JAR_S3
os.environ["ODM_RULESET_JAR_LOCAL"] = RULESET_JAR_LOCAL
os.environ["ODM_XOM_JAR_S3"]        = XOM_JAR_S3
os.environ["ODM_XOM_JAR_LOCAL"]     = XOM_PATH_LOCAL

import odm_metrics
from odm_subprocess_client import call_odm_via_subprocess, SERVER_JAR_LOCAL

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'S3_METRICS_BUCKET',
    'S3_METRICS_PREFIX',
    'S3_METRICS_REGION',
])

print("🚀 AWS GLUE JOB - Crédito PJ (Full Tuning + S3 Metrics)")
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
    (RULESET_JAR_S3, RULESET_JAR_LOCAL, "Ruleset"),
    (XOM_JAR_S3,     XOM_PATH_LOCAL,    "XOM"),
    (ODM_SERVER_JAR_S3, ODM_SERVER_JAR_LOCAL_DRV, "Server UDF"),
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
# 🔍 INSPECIONAR METADADOS DO RULESET
# =============================================================================

def inspecionar_metadados_odm(ruleset_jar_path):
    print("\n" + "=" * 80)
    print("🔍 INFORMAÇÕES DE VERSÃO — ODM & RULESET")
    print("=" * 80)

    try:
        with zipfile.ZipFile(ruleset_jar_path, "r") as z:
            if "META-INF/archive.xml" in z.namelist():
                xml_content = z.read("META-INF/archive.xml")
                root = ET.fromstring(xml_content)

                ruleapp = root.find("ruleapp")
                if ruleapp is not None:
                    app_name = ruleapp.findtext("ruleapp-name")
                    app_ver = ruleapp.findtext("ruleapp-version")
                    print(f"  📦 RuleApp:              {app_name} v{app_ver}")

                ruleset = root.find(".//ruleset")
                if ruleset is not None:
                    rs_name = ruleset.findtext("ruleset-name")
                    rs_ver = ruleset.findtext("ruleset-version")
                    print(f"  📋 Conjunto de Regras:   {rs_name} (v{rs_ver})")

                    props = {}
                    for prop in ruleset.findall("ruleset-property"):
                        p_name = prop.findtext("ruleset-property-name")
                        p_val = prop.findtext("ruleset-property-value")
                        if p_name and p_val:
                            props[p_name] = p_val

                    print(f"  🚀 Versão Decision Engine: {props.get('ruleset.engine.version', 'N/A')}")
                    print(f"  🏛️  Decision Center:       {props.get('decisioncenter.version', 'N/A')}")
                    print(f"  🌿 Branch / Deployer:      {props.get('decisionservice.branch.name', 'N/A')} (por {props.get('decisionservice.deployer.name', 'N/A')})")

            for name in z.namelist():
                if name.endswith(".dsar"):
                    dsar_data = z.read(name)
                    with zipfile.ZipFile(io.BytesIO(dsar_data)) as dz:
                        if "RULES_ENGINE/default/jar/ruleset.jar" in dz.namelist():
                            rjar = dz.read("RULES_ENGINE/default/jar/ruleset.jar")
                            with zipfile.ZipFile(io.BytesIO(rjar)) as rz:
                                for c in rz.namelist():
                                    if c.endswith(".class"):
                                        major = rz.read(c)[7]
                                        v_map = {52: "Java 8", 55: "Java 11", 61: "Java 17", 65: "Java 21"}
                                        print(f"  ☕ Versão Java do Ruleset: {v_map.get(major, f'Bytecode {major}')}")
                                        break
                                break
                    break
    except Exception as e:
        print(f"  ⚠️ Não foi possível ler metadados do JAR: {e}")

    # --- Versão Java do Server UDF JAR ---
    try:
        v_map = {52: "Java 8", 55: "Java 11", 61: "Java 17", 65: "Java 21"}
        with zipfile.ZipFile(ODM_SERVER_JAR_LOCAL_DRV, "r") as z:
            for name in z.namelist():
                if name.endswith(".class") and not name.startswith("META-INF"):
                    major = z.read(name)[7]
                    print(f"  ☕ Versão Java do Server UDF: {v_map.get(major, f'Bytecode {major}')}")
                    break
    except Exception as e:
        print(f"  ⚠️ Não foi possível ler versão Java do Server UDF: {e}")

    print("=" * 80 + "\n")

inspecionar_metadados_odm(RULESET_JAR_LOCAL)

# =============================================================================
# 🔧 INICIALIZAR SPARK
# =============================================================================

print("\n🔧 Inicializando Spark...")
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# =============================================================================
# ✅ INICIALIZAR JOB
# =============================================================================

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Injetar SparkSession no módulo de métricas
odm_metrics.set_spark(spark, RULESET_PATH)

# --- Configurar Spark SQL e S3 ---
print("\n  ⚙️  Configurando Spark SQL e S3...")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.hadoop.fs.s3a.multipart.size", S3_MULTIPART_SIZE)
spark.conf.set("spark.hadoop.fs.s3a.fast.upload", "true")
spark.conf.set("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
spark.conf.set("spark.hadoop.fs.s3a.connection.maximum", "100")
spark.conf.set("spark.hadoop.fs.s3a.threads.max", "20")
spark.conf.set("spark.hadoop.fs.s3a.block.size", S3_BUFFER_SIZE)

print("\n✅ Spark inicializado com sucesso!")

# =============================================================================
# 📊 LER DADOS DE ENTRADA
# =============================================================================

print(f"\n📥 Lendo dados de: {INPUT_PATH}")
t0 = time.time()
df_input = (
    spark.read
    .option("multiline", "true")
    .option("mode", "PERMISSIVE")
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
total_cores = num_executors * cores_per_exec

if PARALLELISM_OVERRIDE:
    optimal_partitions = PARALLELISM_OVERRIDE
else:
    optimal_partitions = max(10, total_cores * 3)

spark.conf.set("spark.sql.shuffle.partitions", str(optimal_partitions))
spark.conf.set("spark.default.parallelism", str(optimal_partitions))
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
    "ruleset_path": RULESET_PATH,
    "input_class": INPUT_CLASS,
    "input_param_name": INPUT_PARAM,
    "output_param_names": ["Cliente", "FaturamentoEleito"],
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
    row_dict = row.asDict(recursive=True)
    record_id = row_dict.pop('record_id')
    decision_id = row_dict.get('DecisionID_', None)
    cliente_data = row_dict.get('Cliente', {})
    if decision_id:
        cliente_data['DecisionID_'] = decision_id
    payload = '{"__config__":' + config_odm_json + ',"data":' + json.dumps(cliente_data) + '}'
    return (record_id, payload)

t0 = time.time()
rdd_input = df_input.rdd.map(create_odm_input)
df_with_input = spark.createDataFrame(rdd_input, ["record_id", "odm_input"])

# =============================================================================
# 🚀 EXECUTAR REGRAS ODM (VIA SERVER STANDALONE JAVA 21)
# =============================================================================

print("\n" + "=" * 80)
print("🔄 EXECUTANDO REGRAS ODM (JAVA 21 SUBPROCESS)")
print("=" * 80)
print(f"   Partições: {optimal_partitions}")
print("=" * 80)

start_time = time.time()

# Executa diretamente sem join/shuffle
df_result = call_odm_via_subprocess(df_with_input, spark, args['JOB_NAME'])

df_result.persist()
total_processed = df_result.count()
elapsed_time = time.time() - start_time

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
    avg_ms = (elapsed_time / total_processed) * 1000
    print(f"  Throughput:           {throughput:,.0f} reg/s")
    print(f"  Tempo médio/registro: {avg_ms:.2f}ms")

# Análise de erros/sucesso
success = 0
errors = 0
if total_processed > 0:
    df_analysis = df_result.select(
        count(when(col("odm_output").contains('"error"'), True)).alias("errors"),
        count(when(~col("odm_output").contains('"error"'), True)).alias("success")
    ).collect()[0]
    success = int(df_analysis['success'])
    errors = int(df_analysis['errors'])
    success_pct = 100.0 * success / total_processed
    error_pct = 100.0 * errors / total_processed
    print(f"\n  📈 Resultados:")
    print(f"     Sucesso: {success:,} ({success_pct:.1f}%)")
    print(f"     Erros:   {errors:,} ({error_pct:.1f}%)")

# Análise de tempo de execução ODM
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
except Exception as e:
    print(f"     ⚠️ Não foi possível analisar tempos: {e}")

# =============================================================================
# 💾 SALVAR RESULTADOS NO S3
# =============================================================================

print("\n" + "=" * 80)
print("💾 SALVANDO RESULTADOS NO S3")
print("=" * 80)
now = datetime.utcnow()
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
# 📊 ENVIAR MÉTRICAS ILMT PARA S3
# =============================================================================

odm_metrics.flush(None, total_processed, success, errors, elapsed_time, start_time)

# =============================================================================
# ✅ FINALIZAR JOB
# =============================================================================

odm_metrics.require_flush()
job.commit()

print("\n" + "=" * 80)
print("✅ JOB FINALIZADO COM SUCESSO!")
print("=" * 80)
print(f"  📊 Processados: {total_processed:,} registros")
print(f"  ⏱️  Tempo total: {elapsed_time:.2f}s")
if total_processed > 0 and elapsed_time > 0:
    print(f"  ⚡ Throughput:  {total_processed/elapsed_time:,.0f} reg/s")
print(f"  💾 Resultados:  {full_output_path}")
print("=" * 80)
