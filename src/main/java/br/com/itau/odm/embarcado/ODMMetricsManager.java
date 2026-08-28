package br.com.itau.odm.embarcado;

/**
 * ODMMetricsManager — envia relatórios ILMT + custom para S3.
 *
 * Uso no driver (1 linha):
 *   ODMMetricsManager.flush(total, ok, errors, durationMs, rulesetPath, startMs, endMs);
 *
 * Bucket, prefix e region são lidos automaticamente das variáveis de ambiente:
 *   S3_METRICS_BUCKET  (obrigatório)
 *   S3_METRICS_PREFIX  (opcional, default: "odm-metrics")
 *   S3_METRICS_REGION  (opcional, default: "us-east-1")
 */
public final class ODMMetricsManager {

    private ODMMetricsManager() {}

    /**
     * Envia relatórios ILMT + custom para S3.
     * Lê bucket/prefix/region das variáveis de ambiente S3_METRICS_*.
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
            String bucket,
            String prefix,
            String region,
            long totalCount,
            long okCount,
            long errorCount,
            long totalDurationMs,
            String rulesetPath,
            long startMs,
            long endMs
    ) {
        if (bucket == null || bucket.trim().isEmpty()) {
            throw new IllegalArgumentException(
                "[ODMMetricsManager] ERRO: 'bucket' é obrigatório.\n" +
                "Configure o job parameter --S3_METRICS_BUCKET no Glue Job."
            );
        }

        String s3Bucket = bucket.trim();
        String s3Prefix = (prefix != null && !prefix.trim().isEmpty()) ? prefix.trim() : "odm-metrics";
        String s3Region = (region != null && !region.trim().isEmpty()) ? region.trim() : "us-east-1";

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
