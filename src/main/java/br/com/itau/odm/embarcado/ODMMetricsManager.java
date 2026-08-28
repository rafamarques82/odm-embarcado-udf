package br.com.itau.odm.embarcado;

/**
 * ODMMetricsManager — envia relatórios ILMT + custom para S3.
 *
 * Uso no driver (2 passos):
 *
 *   1. No início do job (valida e aborta se não configurado):
 *      ODMMetricsManager.init(bucket, prefix, region);
 *
 *   2. No fim do job (envia os relatórios):
 *      ODMMetricsManager.flush(total, ok, errors, durationMs, ruleset, startMs, endMs);
 */
public final class ODMMetricsManager {

    private static volatile String s3Bucket = null;
    private static volatile String s3Prefix = "odm-metrics";
    private static volatile String s3Region  = "us-east-1";

    private ODMMetricsManager() {}

    /**
     * Valida e armazena a configuração S3.
     * Deve ser chamado NO INÍCIO do job — aborta imediatamente se bucket inválido.
     *
     * @param bucket  Bucket S3 de destino (obrigatório)
     * @param prefix  Prefixo/pasta no bucket (opcional, default: "odm-metrics")
     * @param region  Região AWS (opcional, default: "us-east-1")
     */
    public static synchronized void init(String bucket, String prefix, String region) {
        if (bucket == null || bucket.trim().isEmpty()) {
            throw new IllegalArgumentException(
                "[ODMMetricsManager] ERRO: 'bucket' é obrigatório.\n" +
                "Configure o job parameter --S3_METRICS_BUCKET no Glue Job."
            );
        }
        s3Bucket = bucket.trim();
        s3Prefix = (prefix != null && !prefix.trim().isEmpty()) ? prefix.trim() : "odm-metrics";
        s3Region = (region != null && !region.trim().isEmpty()) ? region.trim() : "us-east-1";

        System.out.println("[ODMMetricsManager] Configurado.");
        System.out.println("[ODMMetricsManager]   Bucket: " + s3Bucket);
        System.out.println("[ODMMetricsManager]   Prefix: " + s3Prefix);
        System.out.println("[ODMMetricsManager]   Region: " + s3Region);
    }

    /**
     * Envia os relatórios ILMT + custom para S3.
     * Requer que init() tenha sido chamado antes — lança IllegalStateException caso contrário.
     *
     * @param totalCount      Total de execuções ODM
     * @param okCount         Execuções sem erro
     * @param errorCount      Execuções com erro
     * @param totalDurationMs Duração total em ms
     * @param rulesetPath     Caminho do ruleset
     * @param startMs         Epoch ms do início
     * @param endMs           Epoch ms do fim
     */
    public static void flush(
            long totalCount,
            long okCount,
            long errorCount,
            long totalDurationMs,
            String rulesetPath,
            long startMs,
            long endMs
    ) {
        if (s3Bucket == null) {
            throw new IllegalStateException(
                "[ODMMetricsManager] ERRO: init() não foi chamado.\n" +
                "Chame ODMMetricsManager.init(bucket, prefix, region) no início do job."
            );
        }

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
}

// Made with Bob
