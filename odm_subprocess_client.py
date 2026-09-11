"""
Cliente Python para o OdmServer (processo Java 21 standalone).

Substitui a UDF Java nativa `execute_odm` por uma chamada via socket TCP local
a um processo Java 21 separado, que hospeda o XU do ODM 9.5 fora da JVM do
Spark/Glue (que roda em Java 8/17 e não pode executar classfiles Java 21).

Uso no job Glue, no lugar de:

    df_result = df_with_input.withColumn("odm_output", expr("execute_odm(odm_input)"))

usar:

    df_result = call_odm_via_subprocess(df_with_input, odm_config)

Estratégia:
  - Um processo OdmServer por EXECUTOR (não por partição, não por registro).
  - `mapPartitions`: a primeira tarefa que roda em cada executor garante que o
    daemon está de pé (double-checked locking com lock de arquivo, porque
    várias tasks/cores do mesmo executor podem cair na mesma função ao mesmo
    tempo) e depois processa todos os registros da partição na mesma conexão
    TCP (keep-alive), evitando reabrir socket por registro.
  - O daemon já baixado nos executores via `sc.addFile`/broadcast do jar e do
    JDK 21 (feito uma vez pelo driver, no início do job).
"""

import json
import os
import socket
import stat
import subprocess
import tarfile
import time
import fcntl
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType

# =============================================================================
# CONFIGURAÇÃO — ajuste para os seus paths reais no S3
# =============================================================================

JDK21_TARBALL_S3  = "s3://bre-laboratorio/embarcado/runtime/amazon-corretto-21-x64-linux-jdk.tar.gz"
ODM_SERVER_JAR_S3 = os.environ.get("ODM_SERVER_JAR_S3",
                        "s3://bre-laboratorio/odm-embarcado-server-21-1.0.0.jar")
RULESET_JAR_LOCAL = "/tmp/bre_visaodorelacionamentobancario.jar"
XOM_JAR_LOCAL = "/tmp/XOM-VisaoDoRelacionamentoBancario-FaturamentoEleito-3.4.0.jar"

# Diretório local no executor (mesmo filesystem efêmero usado hoje para os jars)
LOCAL_BASE = "/tmp/odm-server-21"
JDK21_DIR = f"{LOCAL_BASE}/jdk21"
SERVER_JAR_LOCAL = f"{LOCAL_BASE}/odm-embarcado-server-21.jar"
LOCK_FILE = f"{LOCAL_BASE}/server.lock"
READY_FILE = f"{LOCAL_BASE}/server.ready"
LOG_FILE = f"{LOCAL_BASE}/server.log"

SERVER_PORT = 7621  # porta fixa local; como é 127.0.0.1 por executor, não colide entre executores
STARTUP_TIMEOUT_S = 90
CONNECT_RETRY_DELAY_S = 0.5
IDLE_TIMEOUT_S = 900  # servidor se encerra sozinho após 15min sem requisições


def _ensure_local_dir():
    os.makedirs(LOCAL_BASE, exist_ok=True)


def _download_from_s3(s3_uri: str, local_path: str, s3_region: str = "sa-east-1"):
    import boto3
    bucket, key = s3_uri.replace("s3://", "").split("/", 1)
    boto3.client("s3", region_name=s3_region).download_file(bucket, key, local_path)


def _ensure_jdk21():
    """Baixa e extrai o JDK 21 no executor, se ainda não estiver presente.
    Idempotente: se o diretório já existe com um java executável, não baixa de novo."""
    java_bin = f"{JDK21_DIR}/bin/java"
    if os.path.exists(java_bin):
        return java_bin

    _ensure_local_dir()
    tarball_path = f"{LOCAL_BASE}/jdk21.tar.gz"
    if not os.path.exists(tarball_path):
        _download_from_s3(JDK21_TARBALL_S3, tarball_path)

    os.makedirs(JDK21_DIR, exist_ok=True)
    with tarfile.open(tarball_path) as tf:
        # O tarball do Corretto extrai para uma subpasta tipo amazon-corretto-21.x.y-linux-x64/
        # então extraímos e localizamos o bin/java resultante.
        tf.extractall(JDK21_DIR)

    # Encontrar o bin/java dentro da subpasta extraída
    for root, dirs, files in os.walk(JDK21_DIR):
        candidate = os.path.join(root, "bin", "java")
        if os.path.isfile(candidate):
            os.chmod(candidate, os.stat(candidate).st_mode | stat.S_IEXEC)
            return candidate

    raise RuntimeError(f"Não foi possível localizar bin/java após extrair {tarball_path}")


def _ensure_server_jar():
    if not os.path.exists(SERVER_JAR_LOCAL):
        _ensure_local_dir()
        _download_from_s3(ODM_SERVER_JAR_S3, SERVER_JAR_LOCAL)
    return SERVER_JAR_LOCAL


def _is_server_up(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1):
            return True
    except OSError:
        return False


def _ensure_server_running():
    """Garante que existe um OdmServer de pé em 127.0.0.1:SERVER_PORT neste executor.
    Usa lock de arquivo (fcntl) porque múltiplas tasks/threads do mesmo executor
    podem chamar isto concorrentemente — só uma deve efetivamente subir o processo."""
    if _is_server_up(SERVER_PORT):
        return

    _ensure_local_dir()
    lock_fd = os.open(LOCK_FILE, os.O_CREAT | os.O_RDWR)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX)

        # Re-checa depois de obter o lock: outra thread pode ter subido o server
        # enquanto esperávamos.
        if _is_server_up(SERVER_PORT):
            return

        java_bin = _ensure_jdk21()
        server_jar = _ensure_server_jar()

        if os.path.exists(READY_FILE):
            os.remove(READY_FILE)

        log_fh = open(LOG_FILE, "a")
        
        # Constrói o classpath incluindo o Server JAR, o Ruleset JAR e o XOM JAR
        cp_elements = [server_jar]
        if os.path.exists(RULESET_JAR_LOCAL):
            cp_elements.append(RULESET_JAR_LOCAL)
        if os.path.exists(XOM_JAR_LOCAL):
            cp_elements.append(XOM_JAR_LOCAL)
        classpath = ":".join(cp_elements)

        subprocess.Popen(
            [
                java_bin,
                "-Xms1g",
                "-Xmx4g",
                "-XX:+UseG1GC",
                "-Dilog.rules.res.xu.maxCacheSize=10",
                "-Dilog.rules.res.xu.forceUptodate=false",
                "-cp", classpath,
                "br.com.itau.odm.embarcado.server.OdmServer",
                str(SERVER_PORT), READY_FILE, str(IDLE_TIMEOUT_S)
            ],
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            start_new_session=True,  # sobrevive independente do ciclo de vida da task Python
        )

        deadline = time.time() + STARTUP_TIMEOUT_S
        while time.time() < deadline:
            if os.path.exists(READY_FILE) and _is_server_up(SERVER_PORT):
                return
            time.sleep(CONNECT_RETRY_DELAY_S)

        raise RuntimeError(
            f"OdmServer não ficou pronto em {STARTUP_TIMEOUT_S}s. Veja {LOG_FILE} no executor."
        )
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        os.close(lock_fd)


class _OdmConnection:
    """Conexão TCP persistente com o OdmServer local, reaproveitada para todos
    os registros de uma partição (keep-alive), com uma tentativa de reconexão
    caso o servidor tenha sido reciclado (idle timeout) entre um registro e outro."""

    def __init__(self):
        self._sock = None
        self._reader = None

    def _connect(self):
        _ensure_server_running()
        self._sock = socket.create_connection(("127.0.0.1", SERVER_PORT), timeout=60)
        self._sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._reader = self._sock.makefile("r", encoding="utf-8", buffering=65536)

    def call(self, payload: str) -> str:
        for attempt in (1, 2):
            try:
                if self._sock is None:
                    self._connect()
                msg = (payload + "\n").encode("utf-8")
                self._sock.sendall(msg)
                line = self._reader.readline()
                if not line:
                    raise ConnectionError("Servidor fechou a conexão sem responder")
                return line.rstrip("\r\n")
            except (OSError, ConnectionError):
                self.close()
                if attempt == 2:
                    raise

    def close(self):
        try:
            if self._sock:
                self._sock.close()
        except OSError:
            pass
        self._sock = None
        self._reader = None


def _process_partition_direct(job_name):
    def _partition_fn(rows):
        import datetime
        conn = _OdmConnection()
        try:
            for row in rows:
                row_dict = row.asDict()
                odm_input = row_dict["odm_input"]
                try:
                    odm_output = conn.call(odm_input)
                except Exception as e:
                    odm_output = json.dumps({
                        "error": str(e),
                        "errorType": type(e).__name__,
                    })
                yield Row(
                    record_id=row_dict["record_id"],
                    odm_input=odm_input,
                    odm_output=odm_output,
                    processing_timestamp=datetime.datetime.now(),
                    job_name=job_name
                )
        finally:
            conn.close()
    return _partition_fn


def call_odm_via_subprocess(df_with_input, spark, job_name="glue_job"):
    """Transforma diretamente sem necessitar de JOIN (elimina shuffle de rede)."""
    from pyspark.sql.types import TimestampType
    
    result_schema = StructType([
        StructField("record_id", df_with_input.schema["record_id"].dataType, False),
        StructField("odm_input", StringType(), True),
        StructField("odm_output", StringType(), True),
        StructField("processing_timestamp", TimestampType(), True),
        StructField("job_name", StringType(), True),
    ])
    return df_with_input.rdd.mapPartitions(_process_partition_direct(job_name)).toDF(result_schema)
