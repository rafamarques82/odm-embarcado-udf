"""
Script Python AWS Glue - Exemplo de configuração S3Metrics

Este script mostra como passar os parâmetros S3 do Glue Job para a UDF Java.
"""

import sys
import os
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# ===== PASSO 1: Obter parâmetros do Glue Job =====
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'S3_METRICS_BUCKET',
    'S3_METRICS_PREFIX', 
    'S3_METRICS_REGION'
])

# ===== PASSO 2: Configurar variáveis de ambiente para Java =====
# IMPORTANTE: Isso deve ser feito ANTES de criar o SparkContext
os.environ['S3_METRICS_BUCKET'] = args['S3_METRICS_BUCKET']
os.environ['S3_METRICS_PREFIX'] = args['S3_METRICS_PREFIX']
os.environ['S3_METRICS_REGION'] = args['S3_METRICS_REGION']

# ===== PASSO 3: Criar contextos Spark/Glue =====
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ===== PASSO 4: Configurar System Properties do Java (alternativa) =====
# Se as variáveis de ambiente não funcionarem, use System Properties:
spark._jvm.System.setProperty("S3_METRICS_BUCKET", args['S3_METRICS_BUCKET'])
spark._jvm.System.setProperty("S3_METRICS_PREFIX", args['S3_METRICS_PREFIX'])
spark._jvm.System.setProperty("S3_METRICS_REGION", args['S3_METRICS_REGION'])

print(f"[GLUE] S3 Metrics configurado:")
print(f"  Bucket: {args['S3_METRICS_BUCKET']}")
print(f"  Prefix: {args['S3_METRICS_PREFIX']}")
print(f"  Region: {args['S3_METRICS_REGION']}")

# ===== SEU CÓDIGO GLUE AQUI =====
# Exemplo: Ler dados e aplicar UDF ODM
# datasource = glueContext.create_dynamic_frame.from_catalog(...)
# result = datasource.apply_mapping(...)

# ===== FINALIZAR =====
job.commit()

# Made with Bob
