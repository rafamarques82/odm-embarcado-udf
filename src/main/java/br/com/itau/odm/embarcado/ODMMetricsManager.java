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
    private static volatile boolean              flushed     = false;

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

        // SparkConf: propagada para TODOS os executores (inclusive elastic scaling)
        // pois faz parte da configuração da aplicação distribuída pelo driver.
        sc.conf().set("spark.odm.metrics.initialized", "true");

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);

        // Propagar referência do Accumulator para os executores que já estão ativos
        final S3MetricsAccumulator acc = accumulator;
        jsc.parallelize(java.util.Arrays.asList(new Integer[sc.defaultParallelism()]),
                        sc.defaultParallelism())
           .foreachPartition(it -> GenericODMUDF.executorAccumulator = acc);


        System.out.println("[ODMMetricsManager] Inicializado — flush automático registrado.");
        System.out.println("[ODMMetricsManager]   Bucket: " + s3Bucket);
        System.out.println("[ODMMetricsManager]   Prefix: " + s3Prefix);
        System.out.println("[ODMMetricsManager]   Region: " + s3Region);
    }

    /**
     * Envia relatórios com os dados fornecidos pelo driver (calculados do DataFrame).
     * Usa bucket/prefix/region configurados pelo init().
     */
    public static void flush(
            long totalCount, long okCount, long errorCount,
            long totalDurationMs, String rulesetPath,
            long startMs, long endMs
    ) {
        if (s3Bucket == null) {
            throw new IllegalStateException(
                "[ODMMetricsManager] ERRO: init() não foi chamado antes do flush()."
            );
        }
        flushed = true; // marcar antes de enviar — mesmo que o envio falhe, o commit pode prosseguir
        if (totalCount == 0) {
            System.out.println("[ODMMetricsManager] Nenhuma execução registrada — flush ignorado.");
            return;
        }
        System.out.println("[ODMMetricsManager] Enviando relatórios ILMT para S3...");
        System.out.printf("[ODMMetricsManager]   Total: %d | OK: %d | Erros: %d | Duração: %dms | Ruleset: %s%n",
                totalCount, okCount, errorCount, totalDurationMs, rulesetPath);
        try {
            S3MetricsAggregator.sendAggregatedMetrics(
                    s3Bucket, s3Prefix, s3Region,
                    totalCount, okCount, errorCount,
                    totalDurationMs, 0L,
                    rulesetPath, startMs, endMs
            );
            System.out.println("[ODMMetricsManager] ✅ Relatórios enviados com sucesso!");
        } catch (Exception e) {
            System.err.println("[ODMMetricsManager] ❌ Erro ao enviar relatórios: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Verifica se flush() foi chamado após init().
     * Deve ser chamado antes de job.commit() — lança IllegalStateException se flush() foi omitido.
     */
    public static void flushRequired() {
        if (s3Bucket != null && !flushed) {
            throw new IllegalStateException(
                "[ODMMetricsManager] ERRO: flush() não foi chamado.\n" +
                "Chame ODMMetricsManager.flush(...) antes de job.commit()."
            );
        }
    }
}

// Made with Bob
