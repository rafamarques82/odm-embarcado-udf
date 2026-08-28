package br.com.itau.odm.embarcado;

import org.apache.spark.SparkContext;

/**
 * ODMMetricsManager — ponto único de controle de métricas no driver Spark.
 *
 * Uso (driver):
 *   1. ODMMetricsManager.init(sparkContext, bucket, prefix, region);
 *   2. ... executar o job normalmente ...
 *   3. ODMMetricsManager.flush();   ← envia relatórios ILMT + custom para S3
 *
 * Os executores chamam GenericODMUDF que faz add() no S3MetricsAccumulator.
 * O Spark propaga os valores ao driver automaticamente após cada stage.
 * O flush() lê o valor consolidado e envia para S3 sem nenhum código Python adicional.
 */
public final class ODMMetricsManager {

    private static volatile S3MetricsAccumulator accumulator = null;
    private static volatile String s3Bucket  = null;
    private static volatile String s3Prefix  = "odm-metrics";
    private static volatile String s3Region  = "us-east-1";
    private static volatile boolean initialized = false;

    private ODMMetricsManager() {}

    /**
     * Inicializa o manager no driver:
     *  - Cria e registra o S3MetricsAccumulator no SparkContext
     *  - Injeta o accumulator na UDF para que os executores possam usá-lo
     *  - Configura destino S3
     *
     * Deve ser chamado UMA vez, após criar o SparkContext e ANTES de executar a UDF.
     *
     * @param sc      SparkContext do job
     * @param bucket  Bucket S3 de destino
     * @param prefix  Prefixo/pasta no bucket (ex: "odm-metrics")
     * @param region  Região AWS (ex: "sa-east-1")
     */
    public static synchronized void init(SparkContext sc, String bucket, String prefix, String region) {
        if (initialized) {
            System.out.println("[ODMMetricsManager] Já inicializado — ignorando chamada duplicada.");
            return;
        }

        s3Bucket = bucket;
        s3Prefix = (prefix != null && !prefix.isEmpty()) ? prefix : "odm-metrics";
        s3Region = (region != null && !region.isEmpty()) ? region : "us-east-1";

        // Criar e registrar accumulator no Spark
        accumulator = new S3MetricsAccumulator();
        sc.register(accumulator, "ODM-Metrics");

        // Injetar na UDF — os executores receberão a referência via serialização do Spark
        GenericODMUDF.metricsAccumulator = accumulator;

        initialized = true;
        System.out.println("[ODMMetricsManager] Inicializado.");
        System.out.println("[ODMMetricsManager]   Bucket: " + s3Bucket);
        System.out.println("[ODMMetricsManager]   Prefix: " + s3Prefix);
        System.out.println("[ODMMetricsManager]   Region: " + s3Region);
    }

    /**
     * Lê as métricas agregadas do accumulator e envia os relatórios ILMT + custom para S3.
     * Deve ser chamado no driver após o job terminar (antes de job.commit()).
     * É seguro chamar mesmo se init() não foi chamado — faz um log e retorna.
     */
    public static void flush() {
        if (!initialized || accumulator == null) {
            System.out.println("[ODMMetricsManager] Não inicializado — flush ignorado.");
            return;
        }

        S3MetricsAccumulator.MetricsData metrics = accumulator.value();

        if (metrics.totalCount == 0) {
            System.out.println("[ODMMetricsManager] Nenhuma execução registrada — flush ignorado.");
            return;
        }

        System.out.println("[ODMMetricsManager] Enviando relatórios ILMT para S3...");
        System.out.printf("[ODMMetricsManager]   Total: %d | OK: %d | Erros: %d | Duração: %dms | Ruleset: %s%n",
                metrics.totalCount, metrics.okCount, metrics.errorCount,
                metrics.totalDurationMs, metrics.rulesetPath);

        try {
            long startMs = metrics.startTimestampMs > 0
                    ? metrics.startTimestampMs
                    : System.currentTimeMillis();
            long endMs = metrics.endTimestampMs > 0
                    ? metrics.endTimestampMs
                    : System.currentTimeMillis();

            S3MetricsAggregator.sendAggregatedMetrics(
                    s3Bucket,
                    s3Prefix,
                    s3Region,
                    metrics.totalCount,
                    metrics.okCount,
                    metrics.errorCount,
                    metrics.totalDurationMs,
                    metrics.totalRulesFired,
                    metrics.rulesetPath,
                    startMs,
                    endMs
            );

            System.out.println("[ODMMetricsManager] ✅ Relatórios enviados com sucesso!");

        } catch (Exception e) {
            System.err.println("[ODMMetricsManager] ❌ Erro ao enviar relatórios: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

// Made with Bob
