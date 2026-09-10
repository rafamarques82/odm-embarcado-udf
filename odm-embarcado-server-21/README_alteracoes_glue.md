# Alterações no script Glue original

## 1. Remover o registro da UDF Java (não roda mais na JVM do Spark)

Remover este bloco:

```python
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
```

Também pode remover o `jsc.addJar(RULESET_JAR_LOCAL)` e `jsc.addJar(XOM_PATH_LOCAL)`
(e o download desses dois jars no início do script) **se** o ruleset/XOM só
forem usados dentro do processo Java 21 standalone agora — eles deixam de
precisar estar no classpath da JVM do Spark. Mantenha o download apenas se
alguma outra parte do job ainda os usa diretamente.

## 2. Importar o novo módulo cliente

No topo do script, junto dos outros imports:

```python
from odm_subprocess_client import call_odm_via_subprocess
```

Esse arquivo (`odm_subprocess_client.py`) precisa estar disponível para todos
os executores. No Glue, isso normalmente é feito via `--extra-py-files`
apontando para um .zip no S3 contendo esse módulo, ou embutindo o conteúdo
como um arquivo adicional do job (aba "Advanced properties" > "Python
library path" no console do Glue, ou `--extra-py-files
s3://.../odm_subprocess_client.zip` no `create_job`).

## 3. Trocar a chamada da UDF pela chamada via subprocess

Onde hoje está:

```python
df_result = (
    df_with_input
    .withColumn("odm_output",           expr("execute_odm(odm_input)"))
    .withColumn("processing_timestamp", current_timestamp())
    .withColumn("job_name",             lit(args['JOB_NAME']))
)

df_result.persist()
total_processed = df_result.count()
elapsed_time    = time.time() - start_time
```

trocar para:

```python
df_odm_output = call_odm_via_subprocess(df_with_input, spark)

df_result = (
    df_with_input
    .join(df_odm_output, on="record_id", how="left")
    .withColumn("processing_timestamp", current_timestamp())
    .withColumn("job_name",             lit(args['JOB_NAME']))
)

df_result.persist()
total_processed = df_result.count()
elapsed_time    = time.time() - start_time
```

Nota: o `join` é necessário porque `mapPartitions` retorna um novo DataFrame
via RDD, então recombinamos por `record_id` para manter as colunas originais
de `df_with_input`. Se preferir evitar o join (mais barato em Spark), dá para
adaptar `_process_partition` em `odm_subprocess_client.py` para já emitir
todas as colunas de entrada + `odm_output` juntas — fica como otimização
posterior, mais fácil validar corretude primeiro com o join.

## 4. Configuração de S3/paths do novo módulo

Em `odm_subprocess_client.py`, ajustar:
  - `JDK21_TARBALL_S3`: suba um tarball do Amazon Corretto 21 (Linux x64) para
    esse path no S3. Baixe uma vez de
    https://docs.aws.amazon.com/corretto/latest/corretto-21-ug/downloads-list.html
    e faça upload — não baixe direto da internet a cada execução do job (mais
    lento e depende de saída de rede liberada no ambiente Glue).
  - `ODM_SERVER_JAR_S3`: o fat-jar gerado pelo novo módulo Maven
    (`odm-embarcado-server-21`), via `mvn clean package`.

## 5. Empacotar e publicar o novo módulo Maven

```bash
cd odm-embarcado-server-21
mvn clean package
aws s3 cp target/odm-embarcado-server-21.jar \
    s3://bre-laboratorio/embarcado/jars/bre-rendaeleita/odm-embarcado-server-21.jar
```

Isso pressupõe que `odm-embarcado-udf-95` já foi buildado antes
(`mvn clean package` nele primeiro), já que o pom do servidor referencia
`../odm-embarcado-udf-95/target/embarcado-95-1.0.0.jar` via `systemPath`.

Ajuste também os `systemPath` de `bre_visaodorelacionamentobancario.jar` e do
XOM no pom do servidor — apontam hoje para uma pasta `lib/` local dentro de
`odm-embarcado-server-21/`, que você precisa popular com esses dois jars antes
do build (os mesmos hoje baixados do S3 pelo script Glue).

## 6. O que NÃO muda

  - Toda a lógica de leitura do JSON de entrada, preparação do payload
    (`create_odm_input`), particionamento adaptativo, estatísticas, escrita em
    Parquet no S3 e métricas ILMT continuam iguais.
  - `RULESET_PATH`, `INPUT_CLASS`, `INPUT_PARAM`, `config_odm` continuam os
    mesmos — o payload JSON enviado ao `OdmServer` é idêntico ao que ia para a
    UDF Java.
