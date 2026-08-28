package br.com.itau.odm.embarcado;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;

/**
 * ODMMetricsManager — envia relatórios ILMT + custom para S3.
 *
 * Uso no driver (1 linha apenas):
 *   ODMMetricsManager.init(sc, bucket, prefix, region);
 *
 * O flush() é disparado automaticamente via SparkListener quando a aplicação termina.
 *
 * Proteção contra ausência de init():
 *   - No driver: init() valida bucket antes de qualquer processamento
 *   - Nos executores: a UDF verifica o broadcast "odm.initialized" — garante que
 *     qualquer executor (inclusive novos por elastic scaling) recebe a flag antes
 *     de executar qualquer task, pois o Spark entrega broadcasts sob demanda.
 */
public final class ODMMetricsManager {

    private static volatile String               s3Bucket    = null;
    private static volatile String               s3Prefix    = "odm-metrics";
    private static volatile String               s3Region    = "us-east-1";
    private static volatile S3MetricsAccumulator accumulator = null;
    private static volatile long                 startMs     = 0L;

    /** Broadcast que sinaliza aos executores que init() foi chamado. */
    static volatile Broadcast<Boolean> initializedBroadcast = null;

    private ODMMetricsManager() {}

    /**
     * Valida as configs S3, registra o Accumulator, cria broadcast de inicialização
     * e registra SparkListener para flush() automático no fim do job.
     *
     * Aborta imediatamente se bucket for inválido — antes de qualquer processamento.
     *
     * @param sc      SparkContext do job
     * @param bucket  Bucket S3 de destino (obrigatório)
     * @param prefix  Prefixo/pasta no bucket (opcional, default: "odm-metrics")
     * @param region  Região AWS (opcional, default: "us-east-1")
     */
    public static synchronized void init(SparkContext sc, String bucket, String prefix, String region) {
        if (bucket == null || bucket.trim().isEmpty()) {
            throw new IllegalArgumentException(
                "[ODMMetricsManager] ERRO: 'bucket' é obrigatório.\n" +
                "Configure o job parameter --S3_METRICS_BUCKET no Glue Job."
            );
        }
        if (sc == null) {
            throw new IllegalArgumentException("[ODMMetricsManager] ERRO: SparkContext não pode ser null.");
        }

        s3Bucket = bucket.trim();
        s3Prefix = (prefix != null && !prefix.trim().isEmpty()) ? prefix.trim() : "odm-metrics";
        s3Region = (region != null && !region.trim().isEmpty()) ? region.trim() : "us-east-1";
        startMs  = System.currentTimeMillis();

        // Registrar Accumulator no SparkContext (driver)
        accumulator = new S3MetricsAccumulator();
        sc.register(accumulator, "ODM-Metrics");

        // Broadcast: entregue a QUALQUER executor (inclusive novos por elastic scaling)
        // antes que ele execute qualquer task que referencie a variável.
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);
        initializedBroadcast = jsc.broadcast(Boolean.TRUE);

        // Propagar referência do Accumulator para os executores que já estão ativos
        final S3MetricsAccumulator acc = accumulator;
        jsc.parallelize(java.util.Arrays.asList(new Integer[sc.defaultParallelism()]),
                        sc.defaultParallelism())
           .foreachPartition(it -> GenericODMUDF.executorAccumulator = acc);

        // SparkListener: flush() automático quando a aplicação termina
        sc.addSparkListener(new SparkListener() {
            @Override
            public void onApplicationEnd(SparkListenerApplicationEnd end) {
                flush();
            }
        });

        System.out.println("[ODMMetricsManager] Inicializado — flush automático registrado.");
        System.out.println("[ODMMetricsManager]   Bucket: " + s3Bucket);
        System.out.println("[ODMMetricsManager]   Prefix: " + s3Prefix);
        System.out.println("[ODMMetricsManager]   Region: " + s3Region);
    }

    /**
     * Lê as métricas do Accumulator e envia os relatórios para S3.
     * Chamado automaticamente pelo SparkListener no fim da aplicação.
     * Também pode ser chamado manualmente se necessário.
     */
    public static void flush() {
        if (s3Bucket == null || accumulator == null) {
            System.out.println("[ODMMetricsManager] flush() ignorado — init() não foi chamado.");
            return;
        }

        S3MetricsAccumulator.MetricsData metrics = accumulator.value();

        if (metrics.totalCount == 0) {
            System.out.println("[ODMMetricsManager] Nenhuma execução registrada — flush ignorado.");
            return;
        }

        long endMs  = metrics.endTimestampMs > 0 ? metrics.endTimestampMs : System.currentTimeMillis();
        long begMs  = metrics.startTimestampMs > 0 ? metrics.startTimestampMs : startMs;

        System.out.println("[ODMMetricsManager] Enviando relatórios ILMT para S3...");
        System.out.printf("[ODMMetricsManager]   Total: %d | OK: %d | Erros: %d | Duração: %dms | Ruleset: %s%n",
                metrics.totalCount, metrics.okCount, metrics.errorCount,
                metrics.totalDurationMs, metrics.rulesetPath);

        try {
            S3MetricsAggregator.sendAggregatedMetrics(
                    s3Bucket, s3Prefix, s3Region,
                    metrics.totalCount, metrics.okCount, metrics.errorCount,
                    metrics.totalDurationMs, metrics.totalRulesFired,
                    metrics.rulesetPath, begMs, endMs
            );
            System.out.println("[ODMMetricsManager] ✅ Relatórios enviados com sucesso!");
        } catch (Exception e) {
            System.err.println("[ODMMetricsManager] ❌ Erro ao enviar relatórios: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

// Made with Bob
