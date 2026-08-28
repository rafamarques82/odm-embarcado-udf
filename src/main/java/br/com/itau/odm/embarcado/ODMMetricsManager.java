package br.com.itau.odm.embarcado;

/**
 * ODMMetricsManager — envia relatórios ILMT + custom para S3 com dados
 * calculados no driver Spark a partir do DataFrame de resultado.
 *
 * Uso (driver, após df_result.count()):
 *   ODMMetricsManager.flushWithData(bucket, prefix, region,
 *       totalCount, okCount, errorCount, totalDurationMs,
 *       totalRulesFired, rulesetPath, startMs, endMs);
 *
 * Nota: campos static não são serializados pelo Spark para os executores,
 * por isso as métricas são calculadas no driver via DataFrame e passadas
 * diretamente para este método.
 */
public final class ODMMetricsManager {

    private ODMMetricsManager() {}

    /**
     * Envia relatórios ILMT + custom para S3 com os valores fornecidos pelo driver.
     *
     * @param bucket         Bucket S3 de destino (obrigatório)
     * @param prefix         Prefixo/pasta no bucket (ex: "odm-metrics")
     * @param region         Região AWS (ex: "sa-east-1")
     * @param totalCount     Total de execuções ODM
     * @param okCount        Execuções sem erro
     * @param errorCount     Execuções com erro
     * @param totalDurationMs Soma de __ExecutionTimeMs__ de todos os registros
     * @param totalRulesFired Total de regras disparadas (0 se não disponível)
     * @param rulesetPath    Caminho do ruleset (ex: "/bre_xxx/1.0/elege_yyy")
     * @param startMs        Epoch ms do início do processamento ODM
     * @param endMs          Epoch ms do fim do processamento ODM
     */
    public static void flushWithData(
            String bucket,
            String prefix,
            String region,
            long totalCount,
            long okCount,
            long errorCount,
            long totalDurationMs,
            long totalRulesFired,
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
                    s3Bucket,
                    s3Prefix,
                    s3Region,
                    totalCount,
                    okCount,
                    errorCount,
                    totalDurationMs,
                    totalRulesFired,
                    rulesetPath,
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
