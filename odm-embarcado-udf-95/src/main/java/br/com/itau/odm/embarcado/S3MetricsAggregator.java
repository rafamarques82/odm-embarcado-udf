package br.com.itau.odm.embarcado;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Comparator;
import java.util.Date;

/**
 * Helper para enviar métricas agregadas do Spark Accumulator para S3.
 * Usado pelo driver do Spark após coletar métricas de todos os executors.
 */
public class S3MetricsAggregator {
    
    /**
     * Envia métricas agregadas para S3 (chamado pelo driver Spark).
     * 
     * @param bucket Nome do bucket S3
     * @param prefix Prefixo/pasta no S3
     * @param region Região AWS
     * @param totalCount Total de execuções
     * @param okCount Execuções com sucesso
     * @param errorCount Execuções com erro
     * @param totalDurationMs Duração total em ms
     * @param totalRulesFired Total de regras disparadas
     * @param rulesetPath Caminho do ruleset
     * @param startTimestampMs Timestamp de início
     * @param endTimestampMs Timestamp de fim
     */
    public static void sendAggregatedMetrics(
            String bucket,
            String prefix,
            String region,
            long totalCount,
            long okCount,
            long errorCount,
            long totalDurationMs,
            long totalRulesFired,
            String rulesetPath,
            long startTimestampMs,
            long endTimestampMs
    ) throws Exception {
        
        System.out.println("[S3MetricsAggregator] Enviando métricas agregadas para S3...");
        System.out.printf("[S3MetricsAggregator] Total: %d, OK: %d, Erro: %d, Duração: %dms%n",
                totalCount, okCount, errorCount, totalDurationMs);
        
        // Criar cliente S3
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(region)
                .withCredentials(new DefaultAWSCredentialsProviderChain())
                .build();
        
        long avgDuration = (totalCount == 0 ? 0 : totalDurationMs / totalCount);
        
        // 1. Gerar XML ILMT oficial
        String ilmtXml = buildIlmtXml(totalCount, startTimestampMs, endTimestampMs);
        sendToS3(s3Client, bucket, prefix, ilmtXml, "ilmt-report", endTimestampMs);
        System.out.println("[S3MetricsAggregator] XML ILMT enviado (decisions=" + totalCount + ")");
        
        // 2. Gerar XML customizado
        String customXml = buildCustomXml(
                totalCount, okCount, errorCount, totalDurationMs, 
                avgDuration, totalRulesFired, rulesetPath, startTimestampMs, endTimestampMs
        );
        sendToS3(s3Client, bucket, prefix, customXml, "custom-report", endTimestampMs);
        System.out.println("[S3MetricsAggregator] XML customizado enviado");
        
        s3Client.shutdown();
        System.out.println("[S3MetricsAggregator] ✅ Métricas agregadas enviadas com sucesso!");
    }
    
    private static String buildIlmtXml(long totalDecisions, long startEpochMs, long endEpochMs) throws Exception {
        Path ilmtDir = Paths.get("./var/ibm/slmtags");
        Files.createDirectories(ilmtDir);
        
        String batchId = "aggregated-" + endEpochMs;
        DecisionMetering dm = new DecisionMetering("dba-metering");
        DecisionMeteringReport rep = dm.createUsageReport(batchId);
        
        LocalDateTime startLdt = LocalDateTime.ofInstant(Instant.ofEpochMilli(startEpochMs), ZoneId.systemDefault());
        LocalDateTime endLdt   = LocalDateTime.ofInstant(Instant.ofEpochMilli(endEpochMs),   ZoneId.systemDefault());
        rep.setStartTimeStamp(startLdt);
        rep.setStopTimeStamp(endLdt);
        rep.setNbDecisions(totalDecisions);
        
        rep.writeILMTFile();
        
        Path latest = Files.list(ilmtDir)
                .filter(Files::isRegularFile)
                .max(Comparator.comparingLong(p -> p.toFile().lastModified()))
                .orElseThrow(() -> new IllegalStateException("Nenhum arquivo ILMT encontrado"));
        
        return Files.readString(latest, StandardCharsets.UTF_8);
    }
    
    private static String buildCustomXml(
            long totalCount, long okCount, long errorCount, long totalDurationMs,
            long avgDuration, long totalRulesFired, String rulesetPath,
            long startMs, long endMs
    ) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        String startTime = sdf.format(new Date(startMs));
        String endTime = sdf.format(new Date(endMs));
        
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<ODMMetrics>\n" +
                "  <Summary>\n" +
                "    <TotalExecucoes>" + totalCount + "</TotalExecucoes>\n" +
                "    <Sucesso>" + okCount + "</Sucesso>\n" +
                "    <Erros>" + errorCount + "</Erros>\n" +
                "    <DuracaoTotalMs>" + totalDurationMs + "</DuracaoTotalMs>\n" +
                "    <DuracaoMediaMs>" + avgDuration + "</DuracaoMediaMs>\n" +
                "    <TotalRegrasDisparadas>" + totalRulesFired + "</TotalRegrasDisparadas>\n" +
                "    <RuleSet>" + (rulesetPath != null ? rulesetPath : "(unknown)") + "</RuleSet>\n" +
                "    <StartTime>" + startTime + "</StartTime>\n" +
                "    <EndTime>" + endTime + "</EndTime>\n" +
                "  </Summary>\n" +
                "</ODMMetrics>";
    }
    
    private static void sendToS3(AmazonS3 s3Client, String bucket, String prefix, 
                                  String content, String type, long timestampMs) throws Exception {
        SimpleDateFormat partitionFormat = new SimpleDateFormat("yyyy/MM/dd/HH");
        String partition = partitionFormat.format(new Date(timestampMs));
        
        String finalPrefix = prefix;
        if (finalPrefix != null && !finalPrefix.isEmpty() && !finalPrefix.endsWith("/")) {
            finalPrefix += "/";
        } else if (finalPrefix == null || finalPrefix.isEmpty()) {
            finalPrefix = "odm-metrics/";
        }
        
        String key = finalPrefix + partition + "/" + type + "-" + timestampMs + ".xml";
        
        byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
        InputStream inputStream = new ByteArrayInputStream(contentBytes);
        
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(contentBytes.length);
        metadata.setContentType("application/xml");
        
        PutObjectRequest request = new PutObjectRequest(bucket, key, inputStream, metadata);
        s3Client.putObject(request);
        
        System.out.println("[S3MetricsAggregator] Arquivo enviado: s3://" + bucket + "/" + key);
    }
}

// Made with Bob
