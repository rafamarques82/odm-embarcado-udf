"""
odm_metrics.py — Módulo de métricas ODM para AWS Glue Jobs.

Uso: basta importar no início do script Glue.
    import odm_metrics

O módulo detecta automaticamente os parâmetros S3_METRICS_* do Glue Job
e envia os relatórios ao S3 quando o job termina (via atexit).

Não requer nenhuma outra linha de código no script.
"""

import atexit
import sys
import time
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

# ─── Estado interno ──────────────────────────────────────────────────────────

_bucket   = None
_prefix   = None
_region   = None
_ruleset  = None
_spark    = None
_jvm      = None
_start_ms = None
_flushed  = False

# ─── Inicialização automática ────────────────────────────────────────────────

def _init():
    """Lê parâmetros do Glue e registra o atexit. Chamado na importação."""
    global _bucket, _prefix, _region, _start_ms

    # Tentar ler parâmetros do Glue via sys.argv
    argv = " ".join(sys.argv)
    _bucket  = _get_arg("S3_METRICS_BUCKET")
    _prefix  = _get_arg("S3_METRICS_PREFIX") or "odm-metrics"
    _region  = _get_arg("S3_METRICS_REGION") or "us-east-1"
    _start_ms = int(time.time() * 1000)

    if not _bucket:
        raise ValueError(
            "[odm_metrics] ERRO: parâmetro --S3_METRICS_BUCKET não configurado.\n"
            "Configure o Job Parameter no Glue Job antes de executar."
        )

    # Registrar atexit — dispara automaticamente quando o processo Python termina
    atexit.register(_flush_on_exit)
    print(f"[odm_metrics] Inicializado — flush automático registrado.")
    print(f"[odm_metrics]   Bucket: {_bucket}")
    print(f"[odm_metrics]   Prefix: {_prefix}")
    print(f"[odm_metrics]   Region: {_region}")


def _get_arg(name):
    """Lê um argumento --NAME do sys.argv."""
    key = f"--{name}"
    args = sys.argv
    for i, arg in enumerate(args):
        if arg == key and i + 1 < len(args):
            return args[i + 1]
        if arg.startswith(f"{key}="):
            return arg.split("=", 1)[1]
    return None


# ─── API pública ─────────────────────────────────────────────────────────────

def set_spark(spark_session, ruleset_path):
    """
    Injeta o SparkSession e o caminho do ruleset.
    Deve ser chamado após criar o SparkSession — 1 linha no script.

        odm_metrics.set_spark(spark, RULESET_PATH)
    """
    global _spark, _jvm, _ruleset
    _spark   = spark_session
    _jvm     = spark_session.sparkContext._jvm
    _ruleset = ruleset_path


def flush(df_result, total_processed, success, errors, elapsed_time_s, start_time_epoch):
    """
    Envia os relatórios de métricas para o S3.
    Chamado automaticamente pelo atexit — mas pode ser chamado manualmente.

    Parâmetros correspondem ao que já existe no script após o processamento.
    """
    global _flushed

    if not _bucket:
        return

    _flushed = True
    end_ms   = int(time.time() * 1000)
    start_ms = int(start_time_epoch * 1000)
    dur_ms   = int(elapsed_time_s * 1000)

    # 1. XML customizado (Python nativo — sem dependência do JAR)
    _send_custom_xml(total_processed, success, errors, dur_ms,
                     _ruleset or "(unknown)", start_ms, end_ms)

    # 2. XML ILMT oficial (via JAR IBM — se JVM disponível)
    if _jvm is not None:
        try:
            _jvm.br.com.itau.odm.embarcado.ODMMetricsManager.flush(
                int(total_processed), int(success), int(errors),
                dur_ms, _ruleset or "(unknown)",
                start_ms, end_ms
            )
        except Exception as e:
            print(f"[odm_metrics] AVISO: erro ao enviar ILMT via JAR: {e}")


def require_flush():
    """
    Lança exceção se flush() não foi chamado após set_spark().
    Chame antes de job.commit().
    """
    if _bucket and not _flushed:
        raise RuntimeError(
            "[odm_metrics] ERRO: flush() não foi chamado.\n"
            "Certifique-se de chamar odm_metrics.flush(...) antes de job.commit()."
        )


# ─── Internos ────────────────────────────────────────────────────────────────

def _flush_on_exit():
    """Callback do atexit — chamado automaticamente quando o processo termina."""
    if not _flushed and _bucket:
        print("[odm_metrics] AVISO: job terminou sem chamar flush() — métricas não enviadas.")


def _send_custom_xml(total, ok, errors, dur_ms, ruleset, start_ms, end_ms):
    """Gera e envia o XML customizado de métricas para o S3."""
    try:
        import boto3

        avg_ms = dur_ms // total if total > 0 else 0

        def fmt(ms):
            return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        xml = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<ODMMetrics>\n'
            '  <Summary>\n'
            f'    <TotalExecucoes>{total}</TotalExecucoes>\n'
            f'    <Sucesso>{ok}</Sucesso>\n'
            f'    <Erros>{errors}</Erros>\n'
            f'    <DuracaoTotalMs>{dur_ms}</DuracaoTotalMs>\n'
            f'    <DuracaoMediaMs>{avg_ms}</DuracaoMediaMs>\n'
            f'    <RuleSet>{ruleset}</RuleSet>\n'
            f'    <StartTime>{fmt(start_ms)}</StartTime>\n'
            f'    <EndTime>{fmt(end_ms)}</EndTime>\n'
            '  </Summary>\n'
            '</ODMMetrics>'
        )

        prefix = _prefix.rstrip("/") + "/"
        partition = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).strftime("%Y/%m/%d/%H")
        key = f"{prefix}{partition}/custom-report-{end_ms}.xml"

        s3 = boto3.client("s3", region_name=_region)
        s3.put_object(Bucket=_bucket, Key=key, Body=xml.encode("utf-8"),
                      ContentType="application/xml")

        print(f"[odm_metrics] ✅ XML customizado enviado: s3://{_bucket}/{key}")

    except Exception as e:
        print(f"[odm_metrics] ❌ Erro ao enviar XML customizado: {e}")


# ─── Auto-init na importação ─────────────────────────────────────────────────

_init()
