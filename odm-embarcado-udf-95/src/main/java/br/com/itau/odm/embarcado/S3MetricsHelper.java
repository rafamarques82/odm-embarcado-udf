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

public final class S3MetricsHelper {
    
    private S3MetricsHelper() {}
    
    public static boolean sendIlmtMetrics(
            String bucketName,
            String prefix,
            String region,
            String rulesetPath,
            long totalDecisions,
            long startTimeMs,
            long endTimeMs) {
        
        try {
            System.out.println("[S3-HELPER] Iniciando envio de metricas ILMT...");
            
            if (bucketName == null || bucketName.isEmpty()) {
                System.err.println("[S3-HELPER] ERRO: Bucket nao configurado");
                return false;
            }
            
            if (totalDecisions <= 0) {
                System.err.println("[S3-HELPER] ERRO: Total de decisoes invalido: " + totalDecisions);
                return false;
            }
            
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(region != null && !region.isEmpty() ? region : "us-east-1")
                    .withCredentials(new DefaultAWSCredentialsProviderChain())
                    .build();
            
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
            String startTime = sdf.format(new Date(startTimeMs));
            String endTime = sdf.format(new Date(endTimeMs));
            
            String ilmtXml = buildIlmtXml(rulesetPath, totalDecisions, startTime, endTime);
            
            long durationMs = endTimeMs - startTimeMs;
            String customXml = buildCustomXml(rulesetPath, totalDecisions, startTime, endTime, durationMs);
            
            SimpleDateFormat partitionFormat = new SimpleDateFormat("yyyy/MM/dd/HH");
            String partition = partitionFormat.format(new Date(startTimeMs));
            long timestamp = startTimeMs;
            
            String finalPrefix = prefix;
            if (finalPrefix != null && !finalPrefix.isEmpty() && !finalPrefix.endsWith("/")) {
                finalPrefix += "/";
            } else if (finalPrefix == null || finalPrefix.isEmpty()) {
                finalPrefix = "odm-metrics/";
            }
            
            String ilmtKey = finalPrefix + partition + "/ilmt-report-" + timestamp + ".xml";
            sendToS3(s3Client, bucketName, ilmtKey, ilmtXml);
            System.out.println("[S3-HELPER] ILMT report enviado: s3://" + bucketName + "/" + ilmtKey);
            
            String customKey = finalPrefix + partition + "/custom-report-" + timestamp + ".xml";
            sendToS3(s3Client, bucketName, customKey, customXml);
            System.out.println("[S3-HELPER] Custom report enviado: s3://" + bucketName + "/" + customKey);
            
            System.out.println("[S3-HELPER] Metricas ILMT enviadas com sucesso!");
            return true;
            
        } catch (Exception e) {
            System.err.println("[S3-HELPER] ERRO ao enviar metricas: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    private static String buildIlmtXml(String rulesetPath, long totalDecisions, String startTime, String endTime) {
        try {
            // Usar DecisionMeteringReport para gerar XML ILMT oficial (mesmo que KafkaMetrics)
            Path ilmtDir = Paths.get("./var/ibm/slmtags");
            Files.createDirectories(ilmtDir);
            
            String batchId = "s3-" + System.currentTimeMillis();
            DecisionMetering dm = new DecisionMetering("dba-metering");
            DecisionMeteringReport rep = dm.createUsageReport(batchId);
            
            // Converter timestamps de String ISO para LocalDateTime
            LocalDateTime startLdt = LocalDateTime.parse(startTime.substring(0, 19));
            LocalDateTime endLdt = LocalDateTime.parse(endTime.substring(0, 19));
            
            rep.setStartTimeStamp(startLdt);
            rep.setStopTimeStamp(endLdt);
            rep.setNbDecisions(totalDecisions);
            
            rep.writeILMTFile();
            
            // Ler o arquivo ILMT gerado mais recente
            Path latest = Files.list(ilmtDir)
                    .filter(Files::isRegularFile)
                    .max(Comparator.comparingLong(p -> p.toFile().lastModified()))
                    .orElseThrow(() -> new IllegalStateException("Nenhum arquivo ILMT encontrado"));
            
            String content = Files.readString(latest, StandardCharsets.UTF_8);
            
            // Extrair cabeçalho + último bloco <Metric> (mesmo que KafkaMetrics)
            String schemaStart = "<SchemaVersion>";
            String softwareStart = "<SoftwareIdentity>";
            String softwareEnd = "</SoftwareIdentity>";
            String metricStart = "<Metric";
            String metricEnd = "</Metric>";
            
            int schemaIdx = content.indexOf(schemaStart);
            int softStartIdx = content.indexOf(softwareStart);
            int softEndIdx = content.indexOf(softwareEnd) + softwareEnd.length();
            
            int lastMetricStartIdx = content.lastIndexOf(metricStart);
            int lastMetricEndIdx = content.indexOf(metricEnd, lastMetricStartIdx) + metricEnd.length();
            
            String header = content.substring(schemaIdx, softEndIdx);
            String lastMetric = content.substring(lastMetricStartIdx, lastMetricEndIdx);
            
            return header + "\n" + lastMetric;
            
        } catch (Exception e) {
            System.err.println("[S3-HELPER] Erro ao gerar XML ILMT: " + e.getMessage());
            e.printStackTrace();
            // Fallback: formato simples
            return buildSimpleIlmtXml(rulesetPath, totalDecisions, startTime, endTime);
        }
    }
    
    private static String buildSimpleIlmtXml(String rulesetPath, long totalDecisions, String startTime, String endTime) {
        StringBuilder xml = new StringBuilder();
        xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        xml.append("<DecisionServiceMetering>\n");
        xml.append("  <RulesetPath>").append(rulesetPath).append("</RulesetPath>\n");
        xml.append("  <StartTime>").append(startTime).append("</StartTime>\n");
        xml.append("  <EndTime>").append(endTime).append("</EndTime>\n");
        xml.append("  <TotalDecisions>").append(totalDecisions).append("</TotalDecisions>\n");
        xml.append("  <ProductName>IBM Operational Decision Manager</ProductName>\n");
        xml.append("  <ProductVersion>8.12.0</ProductVersion>\n");
        xml.append("</DecisionServiceMetering>\n");
        return xml.toString();
    }
    
    private static String buildCustomXml(String rulesetPath, long totalDecisions, 
                                        String startTime, String endTime, long durationMs) {
        double avgDurationMs = totalDecisions > 0 ? (double) durationMs / totalDecisions : 0;
        
        StringBuilder xml = new StringBuilder();
        xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        xml.append("<ODMExecutionSummary>\n");
        xml.append("  <RulesetPath>").append(rulesetPath).append("</RulesetPath>\n");
        xml.append("  <TimeWindow>\n");
        xml.append("    <Start>").append(startTime).append("</Start>\n");
        xml.append("    <End>").append(endTime).append("</End>\n");
        xml.append("  </TimeWindow>\n");
        xml.append("  <Executions>\n");
        xml.append("    <Total>").append(totalDecisions).append("</Total>\n");
        xml.append("    <Success>").append(totalDecisions).append("</Success>\n");
        xml.append("    <Errors>0</Errors>\n");
        xml.append("  </Executions>\n");
        xml.append("  <Performance>\n");
        xml.append("    <TotalDurationMs>").append(durationMs).append("</TotalDurationMs>\n");
        xml.append("    <AverageDurationMs>").append(String.format("%.2f", avgDurationMs)).append("</AverageDurationMs>\n");
        xml.append("  </Performance>\n");
        xml.append("</ODMExecutionSummary>\n");
        return xml.toString();
    }
    
    private static void sendToS3(AmazonS3 s3Client, String bucket, String key, String content) throws Exception {
        byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
        InputStream inputStream = new ByteArrayInputStream(contentBytes);
        
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(contentBytes.length);
        metadata.setContentType("application/xml");
        
        PutObjectRequest request = new PutObjectRequest(bucket, key, inputStream, metadata);
        s3Client.putObject(request);
    }
}
