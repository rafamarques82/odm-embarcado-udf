import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import boto3
from datetime import datetime
import json

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

# Configurar System Properties para S3Metrics (caso queira usar a classe Java também)
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
print(f"[GLUE] Início do processamento: {start_time.isoformat()}")

# Forçar execução e contar resultados
result_count = df_result.count()
end_time = datetime.now()

print(f"[GLUE] Fim do processamento: {end_time.isoformat()}")
print(f"[GLUE] Total de registros processados: {result_count}")

# Calcular duração
duration_seconds = (end_time - start_time).total_seconds()
print(f"[GLUE] Duração total: {duration_seconds:.2f} segundos")

# Mostrar alguns resultados
print("[GLUE] Primeiros 5 resultados:")
df_result.show(5, truncate=False)

# ============================================
# ENVIAR MÉTRICAS ILMT PARA S3
# ============================================

def send_ilmt_metrics_to_s3(
    bucket_name,
    prefix,
    region,
    ruleset_path,
    total_decisions,
    start_time,
    end_time
):
    """
    Envia relatório ILMT para S3 em formato XML.
    """
    try:
        # Criar cliente S3
        s3_client = boto3.client('s3', region_name=region)
        
        # Formatar timestamps
        start_str = start_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + start_time.strftime('%z')
        end_str = end_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + end_time.strftime('%z')
        
        # Gerar XML ILMT
        ilmt_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<DecisionServiceMetering>
  <RulesetPath>{ruleset_path}</RulesetPath>
  <StartTime>{start_str}</StartTime>
  <EndTime>{end_str}</EndTime>
  <TotalDecisions>{total_decisions}</TotalDecisions>
  <ProductName>IBM Operational Decision Manager</ProductName>
  <ProductVersion>8.12.0</ProductVersion>
</DecisionServiceMetering>
"""
        
        # Gerar XML customizado com mais detalhes
        custom_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<ODMExecutionSummary>
  <RulesetPath>{ruleset_path}</RulesetPath>
  <TimeWindow>
    <Start>{start_str}</Start>
    <End>{end_str}</End>
  </TimeWindow>
  <Executions>
    <Total>{total_decisions}</Total>
    <Success>{total_decisions}</Success>
    <Errors>0</Errors>
  </Executions>
  <Performance>
    <TotalDurationSeconds>{duration_seconds:.2f}</TotalDurationSeconds>
    <AverageDurationMs>{(duration_seconds * 1000 / total_decisions):.2f}</AverageDurationMs>
  </Performance>
</ODMExecutionSummary>
"""
        
        # Criar partição por data/hora
        partition = start_time.strftime('%Y/%m/%d/%H')
        timestamp = int(start_time.timestamp() * 1000)
        
        # Enviar ILMT report
        ilmt_key = f"{prefix}{partition}/ilmt-report-{timestamp}.xml"
        s3_client.put_object(
            Bucket=bucket_name,
            Key=ilmt_key,
            Body=ilmt_xml.encode('utf-8'),
            ContentType='application/xml'
        )
        print(f"[S3-METRICS] ✅ ILMT report enviado: s3://{bucket_name}/{ilmt_key}")
        
        # Enviar custom report
        custom_key = f"{prefix}{partition}/custom-report-{timestamp}.xml"
        s3_client.put_object(
            Bucket=bucket_name,
            Key=custom_key,
            Body=custom_xml.encode('utf-8'),
            ContentType='application/xml'
        )
        print(f"[S3-METRICS] ✅ Custom report enviado: s3://{bucket_name}/{custom_key}")
        
        return True
        
    except Exception as e:
        print(f"[S3-METRICS] ❌ ERRO ao enviar métricas: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

# Enviar métricas para S3
print("\n[GLUE] Enviando métricas ILMT para S3...")
success = send_ilmt_metrics_to_s3(
    bucket_name=args['S3_METRICS_BUCKET'],
    prefix=args.get('S3_METRICS_PREFIX', 'odm-metrics/'),
    region=args.get('S3_METRICS_REGION', 'us-east-1'),
    ruleset_path=args['RULESET_PATH'],
    total_decisions=result_count,
    start_time=start_time,
    end_time=end_time
)

if success:
    print(f"\n[GLUE] ✅ Job concluído com sucesso!")
    print(f"  - Registros processados: {result_count}")
    print(f"  - Duração: {duration_seconds:.2f}s")
    print(f"  - Métricas ILMT enviadas para S3")
else:
    print(f"\n[GLUE] ⚠️  Job concluído mas houve erro ao enviar métricas")

job.commit()

# Made with Bob
