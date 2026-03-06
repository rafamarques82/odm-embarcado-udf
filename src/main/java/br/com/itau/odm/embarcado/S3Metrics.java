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
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * S3Metrics (singleton, thread-safe)
 * 
 * Similar ao KafkaMetrics, mas envia métricas para S3.
 * - Lê "odm-s3.properties" (-Dodm.s3.props / ENV ODM_S3_PROPS / ./ / classpath)
 * - Acumula execuções e, no shutdown/close, publica ILMT + XML customizado para S3
 * - Validação no boot: verifica se bucket existe e tem permissão de escrita
 */
final class S3Metrics {

    /* ===== Estado de ciclo de vida ===== */
    private static final AtomicBoolean INIT   = new AtomicBoolean(false);
    private static final AtomicBoolean HOOKED = new AtomicBoolean(false);
    private static final AtomicBoolean CLOSED = new AtomicBoolean(false);
    private static volatile boolean S3_READY = false;

    /* ===== S3 ===== */
    private static AmazonS3 S3_CLIENT;
    private static String BUCKET_NAME;
    private static String PREFIX;
    private static String REGION;

    /* ===== Último erro (diagnóstico) ===== */
    private static volatile String LAST_ERROR_KIND = null;
    private static volatile String LAST_ERROR_MSG  = null;

    /* ===== Acumuladores (thread-safe) ===== */
    private static final AtomicLong TOTAL_COUNT        = new AtomicLong(0);
    private static final AtomicLong OK_COUNT           = new AtomicLong(0);
    private static final AtomicLong ERROR_COUNT        = new AtomicLong(0);
    private static final AtomicLong TOTAL_DURATION_MS  = new AtomicLong(0);
    private static final AtomicLong TOTAL_RULES_FIRED  = new AtomicLong(0);
    private static final AtomicBoolean SUMMARY_SENT    = new AtomicBoolean(false);
    
    /* ===== Flush periódico ===== */
    private static final long FLUSH_INTERVAL = 10000; // Enviar a cada 10k execuções
    private static final AtomicLong LAST_FLUSH_COUNT = new AtomicLong(0);

    /* ===== Janela de tempo da execução agregada ===== */
    private static final AtomicLong START_TS_MS = new AtomicLong(0L);
    private static final AtomicLong END_TS_MS   = new AtomicLong(0L);

    /* ===== Ruleset dinâmico (atualizado pelo façade) ===== */
    private static volatile String RULESET_PATH = "(unknown)";

    private S3Metrics() {}

    /* ===================== API pública ===================== */

    /** Valida S3 no boot */
    static void validateOrThrow() {
        initIfNeeded();
        
        if (!S3_READY) {
            String kind = (LAST_ERROR_KIND != null) ? LAST_ERROR_KIND : "desconhecido";
            String msg  = (LAST_ERROR_MSG  != null) ? LAST_ERROR_MSG  : "Falha não especificada ao validar S3.";
            System.err.println("[ODM-S3] AVISO (" + kind + "): " + msg + " - Métricas S3 desabilitadas.");
        }
    }

    /** Registra UMA execução e faz flush periódico a cada FLUSH_INTERVAL execuções */
    static void recordExecution(String rulesetPath, long durationMs, Integer rulesFired, boolean success) {
        // Atualizar ruleset se fornecido
        if (rulesetPath != null && !rulesetPath.isBlank()) {
            RULESET_PATH = rulesetPath;
        }
        
        long currentCount = TOTAL_COUNT.incrementAndGet();
        if (success) OK_COUNT.incrementAndGet();
        else ERROR_COUNT.incrementAndGet();

        if (durationMs > 0) TOTAL_DURATION_MS.addAndGet(durationMs);
        if (rulesFired != null && rulesFired > 0) TOTAL_RULES_FIRED.addAndGet(rulesFired);

        long now = System.currentTimeMillis();
        START_TS_MS.compareAndSet(0L, now);
        END_TS_MS.set(now);
        
        // Flush periódico: a cada FLUSH_INTERVAL execuções, enviar snapshot para S3
        long lastFlush = LAST_FLUSH_COUNT.get();
        if (currentCount - lastFlush >= FLUSH_INTERVAL) {
            if (LAST_FLUSH_COUNT.compareAndSet(lastFlush, currentCount)) {
                // Apenas uma thread fará o flush
                try {
                    flushPeriodicSnapshot();
                } catch (Exception e) {
                    System.err.println("[ODM-S3] Erro no flush periódico: " + e.getMessage());
                }
            }
        }
    }
    
    /** Sobrecarga para compatibilidade com KafkaMetrics */
    static void recordExecution(String status, long durationMs, Integer rulesFired) {
        recordExecution(null, durationMs, rulesFired, "ok".equalsIgnoreCase(status));
    }

    /** Atualiza o ruleset dinâmico a partir do façade. */
    static void updateRuleset(String rulesetPath) {
        if (rulesetPath != null && !rulesetPath.isBlank()) RULESET_PATH = rulesetPath;
    }

    /** Fecha: envia o RESUMO (XML ILMT + XML customizado) para S3 */
    static void close() { safeFlushAndClose(); }

    /* ===================== Internals ===================== */

    static boolean isReady() { return S3_READY; }

    static void initIfNeeded() {
        if (INIT.get()) return;

        synchronized (S3Metrics.class) {
            if (INIT.get()) return;

            try {
                Properties props = loadS3Properties();
                BUCKET_NAME = props.getProperty("bucket");
                PREFIX = props.getProperty("prefix", "odm-metrics");
                REGION = props.getProperty("region", "us-east-1");

                if (BUCKET_NAME == null || BUCKET_NAME.isEmpty()) {
                    setError("config", "Propriedade 'bucket' ausente ou vazia no arquivo .properties");
                    return;
                }

                // Criar cliente S3
                S3_CLIENT = AmazonS3ClientBuilder.standard()
                        .withRegion(REGION)
                        .withCredentials(new DefaultAWSCredentialsProviderChain())
                        .build();

                // Validar bucket (verifica se existe e tem permissão)
                if (!S3_CLIENT.doesBucketExistV2(BUCKET_NAME)) {
                    setError("bucket", "Bucket '" + BUCKET_NAME + "' não existe ou sem permissão de leitura");
                    return;
                }

                // Teste de escrita (probe)
                probeSendOrStop();

                S3_READY = true;
                clearError();
                System.out.println("[ODM-S3] Conexão validada. Bucket: " + BUCKET_NAME + ", Prefix: " + PREFIX);

            } catch (Exception e) {
                setError("init", "Erro ao inicializar S3Metrics: " + e.getMessage());
                e.printStackTrace();
            } finally {
                INIT.set(true);

                if (!S3_READY) {
                    String kind = (LAST_ERROR_KIND != null) ? LAST_ERROR_KIND : "init";
                    String msg  = (LAST_ERROR_MSG  != null) ? LAST_ERROR_MSG  : "S3 não validado no boot.";
                    System.err.println("[ODM-S3] AVISO (" + kind + "): " + msg + " - Métricas S3 desabilitadas.");
                }
            }
        }

        // Hook único: envia resumo no shutdown
        registerShutdownHookOnce();
    }

    private static void registerShutdownHookOnce() {
        if (HOOKED.compareAndSet(false, true)) {
            Runtime.getRuntime().addShutdownHook(new Thread(S3Metrics::safeFlushAndClose, "odm-s3-shutdown"));
        }
    }

    private static void safeFlushAndClose() {
        try { flushSummaryOnce(); }
        catch (Exception e) { System.err.println("[ODM-S3] Erro ao enviar resumo no shutdown: " + e.getMessage()); }
        finally { closeClient(); }
    }

    private static void closeClient() {
        if (CLOSED.compareAndSet(false, true)) {
            try {
                if (S3_CLIENT != null) {
                    S3_CLIENT.shutdown();
                }
            } catch (Exception ignore) {}
        }
    }

    /** Envia snapshot periódico (não reseta contadores) */
    private static void flushPeriodicSnapshot() {
        if (!S3_READY) return;
        
        try {
            final long totalCount    = TOTAL_COUNT.get();
            final long okCount       = OK_COUNT.get();
            final long errorCount    = ERROR_COUNT.get();
            final long totalDuration = TOTAL_DURATION_MS.get();
            final long avgDuration   = (totalCount == 0 ? 0 : totalDuration / totalCount);
            final long startMs       = (START_TS_MS.get() == 0 ? System.currentTimeMillis() : START_TS_MS.get());
            final long endMs         = System.currentTimeMillis();
            final String rulesetSnap = RULESET_PATH;
            
            System.out.printf("[ODM-S3] Flush periódico: total=%d ok=%d err=%d durMs=%d avg=%d ruleset=%s%n",
                    totalCount, okCount, errorCount, totalDuration, avgDuration, rulesetSnap);
            
            // Enviar XMLs para S3 (snapshot parcial)
            String ilmtXml = buildIlmtXmlWithDecisionMetering(totalCount, startMs, endMs);
            String customXml = buildCustomXml(totalCount, okCount, errorCount, totalDuration, avgDuration, rulesetSnap, endMs);
            
            sendToS3(ilmtXml, "ilmt-periodic", endMs);
            System.out.println("[ODM-S3] XML ILMT periódico enviado (decisions=" + totalCount + ")");
            
            sendToS3(customXml, "custom-periodic", endMs);
            System.out.println("[ODM-S3] XML customizado periódico enviado");
            
        } catch (Exception e) {
            System.err.println("[ODM-S3] Erro no flush periódico: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /** Envia o RESUMO uma única vez */
    private static void flushSummaryOnce() {
        if (!SUMMARY_SENT.compareAndSet(false, true)) return;
        if (CLOSED.get()) {
            System.err.println("[ODM-S3] Cliente já fechado — resumo NÃO enviado (contagem local: "
                    + TOTAL_COUNT.get() + ").");
            return;
        }

        initIfNeeded();
        if (!S3_READY) {
            System.err.println("[ODM-S3] S3 não validado — resumo NÃO enviado (contagem local: "
                    + TOTAL_COUNT.get() + ").");
            return;
        }

        try {
            final long totalCount    = TOTAL_COUNT.get();
            final long okCount       = OK_COUNT.get();
            final long errorCount    = ERROR_COUNT.get();
            final long totalDuration = TOTAL_DURATION_MS.get();
            final long avgDuration   = (totalCount == 0 ? 0 : totalDuration / totalCount);
            final long startMs       = (START_TS_MS.get() == 0 ? System.currentTimeMillis() : START_TS_MS.get());
            final long endMs         = (END_TS_MS.get()   == 0 ? System.currentTimeMillis() : END_TS_MS.get());
            final String rulesetSnap = RULESET_PATH;

            System.out.printf("[ODM-S3] Snapshot resumo: total=%d ok=%d err=%d durMs=%d avg=%d ruleset=%s%n",
                    totalCount, okCount, errorCount, totalDuration, avgDuration, rulesetSnap);

            // 1) XML ILMT
            final String ilmtXml = buildIlmtXmlWithDecisionMetering(totalCount, startMs, endMs);
            sendToS3(ilmtXml, "ilmt", startMs);
            System.out.println("[ODM-S3] XML ILMT enviado (decisions=" + totalCount + ")");

            // 2) XML CUSTOMIZADO
            final String customXml = buildCustomXml(
                    totalCount, okCount, errorCount, totalDuration, avgDuration, rulesetSnap, System.currentTimeMillis()
            );
            sendToS3(customXml, "custom", startMs);
            System.out.println("[ODM-S3] XML customizado enviado");

        } catch (Exception e) {
            System.err.println("[ODM-S3] Erro ao montar/enviar XMLs: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /* ===================== Probe ===================== */

    private static void probeSendOrStop() {
        try {
            String probeContent = "ping-" + System.currentTimeMillis();
            String probeKey = (PREFIX.endsWith("/") ? PREFIX : PREFIX + "/") + "__odm_probe__.txt";
            
            byte[] contentBytes = probeContent.getBytes(StandardCharsets.UTF_8);
            InputStream inputStream = new ByteArrayInputStream(contentBytes);
            
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(contentBytes.length);
            metadata.setContentType("text/plain");
            
            PutObjectRequest request = new PutObjectRequest(BUCKET_NAME, probeKey, inputStream, metadata);
            S3_CLIENT.putObject(request);
            
        } catch (Exception e) {
            setError("send", "Falha ao enviar 'probe' ao S3: " + e.getMessage());
            throw new RuntimeException("S3 probe failed", e);
        }
    }

    /* ===================== Utilitários ===================== */

    private static void setError(String kind, String message) {
        S3_READY = false;
        LAST_ERROR_KIND = kind;
        LAST_ERROR_MSG  = message;
        System.err.println("[ODM-S3] " + kind + ": " + message);
    }

    private static void clearError() {
        LAST_ERROR_KIND = null;
        LAST_ERROR_MSG  = null;
    }

    private static Properties loadS3Properties() throws Exception {
        Properties p = new Properties();

        // 1. Tentar carregar de arquivo (odm-s3.properties)
        String explicitPath = System.getProperty("odm.s3.props");
        if (explicitPath == null || explicitPath.isBlank()) {
            explicitPath = System.getenv("ODM_S3_PROPS");
        }

        boolean loadedFromFile = false;
        if (explicitPath != null && !explicitPath.isBlank() && Files.exists(Paths.get(explicitPath))) {
            try (InputStream in = Files.newInputStream(Paths.get(explicitPath))) {
                p.load(in);
                loadedFromFile = true;
            }
        } else if (Files.exists(Paths.get("odm-s3.properties"))) {
            try (InputStream in = Files.newInputStream(Paths.get("odm-s3.properties"))) {
                p.load(in);
                loadedFromFile = true;
            }
        } else {
            try (InputStream in = S3Metrics.class.getClassLoader().getResourceAsStream("odm-s3.properties")) {
                if (in != null) {
                    p.load(in);
                    loadedFromFile = true;
                }
            }
        }

        // 2. Sobrescrever com System Properties (se existirem) - compatibilidade com script Glue
        String sysBucket = System.getProperty("S3_METRICS_BUCKET");
        String sysPrefix = System.getProperty("S3_METRICS_PREFIX");
        String sysRegion = System.getProperty("S3_METRICS_REGION");
        
        if (sysBucket != null && !sysBucket.isBlank()) {
            p.setProperty("bucket", sysBucket);
            loadedFromFile = true; // considera como configurado
        }
        if (sysPrefix != null && !sysPrefix.isBlank()) {
            p.setProperty("prefix", sysPrefix);
        }
        if (sysRegion != null && !sysRegion.isBlank()) {
            p.setProperty("region", sysRegion);
        }

        // 3. Validar configuração mínima
        if (p.getProperty("bucket") == null || p.getProperty("bucket").isBlank()) {
            throw new IllegalArgumentException(
                "Propriedade 'bucket' ausente. Configure via:\n" +
                "  - Arquivo odm-s3.properties, ou\n" +
                "  - System Property: -DS3_METRICS_BUCKET=nome-do-bucket"
            );
        }

        // 4. Defaults
        if (p.getProperty("prefix") == null || p.getProperty("prefix").isBlank()) {
            p.setProperty("prefix", "odm-metrics");
        }
        if (p.getProperty("region") == null || p.getProperty("region").isBlank()) {
            p.setProperty("region", "us-east-1");
        }

        return p;
    }

    /* ===================== ILMT ===================== */

    private static String buildIlmtXmlWithDecisionMetering(long totalDecisions, long startEpochMs, long endEpochMs) throws Exception {
        Path ilmtDir = Paths.get("./var/ibm/slmtags");
        Files.createDirectories(ilmtDir);

        String batchId = "s3-" + endEpochMs;
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

        String content = Files.readString(latest, StandardCharsets.UTF_8);

        String schemaStart   = "<SchemaVersion>";
        String softwareStart = "<SoftwareIdentity>";
        String softwareEnd   = "</SoftwareIdentity>";
        String metricStart   = "<Metric";
        String metricEnd     = "</Metric>";

        int schemaIdx    = content.indexOf(schemaStart);
        int softStartIdx = content.indexOf(softwareStart);
        int softEndIdx   = content.indexOf(softwareEnd) + softwareEnd.length();

        int lastMetricStartIdx = content.lastIndexOf(metricStart);
        int lastMetricEndIdx = content.indexOf(metricEnd, lastMetricStartIdx) + metricEnd.length();

        String header     = content.substring(schemaIdx, softEndIdx);
        String lastMetric = content.substring(lastMetricStartIdx, lastMetricEndIdx);

        return header + "\n" + lastMetric;
    }

    /* ===================== XML Customizado ===================== */
    
    private static String buildCustomXml(
            long totalCount,
            long okCount,
            long errorCount,
            long totalDurationMs,
            long avgDurationMs,
            String rulesetPath,
            long tsEpochMs
    ) {
        String tsIso = Instant.ofEpochMilli(tsEpochMs).toString();

        StringBuilder sb = new StringBuilder(512);
        sb.append("<Evento>\n")
          .append(" <Id>evt-").append(tsEpochMs).append("</Id>\n")
          .append(" <Timestamp>").append(tsIso).append("</Timestamp>\n")
          .append(" <Origem>S3Metrics</Origem>\n")
          .append(" <Payload>\n")
          .append("  <TotalExecucoes>").append(totalCount).append("</TotalExecucoes>\n")
          .append("  <ExecutadasComSucesso>").append(okCount).append("</ExecutadasComSucesso>\n")
          .append("  <Falhas>").append(errorCount).append("</Falhas>\n")
          .append("  <DuracaoTotalMs>").append(totalDurationMs).append("</DuracaoTotalMs>\n")
          .append("  <MediaPorExecucaoMs>").append(avgDurationMs).append("</MediaPorExecucaoMs>\n")
          .append("  <RuleSet>").append(rulesetPath == null ? "" : rulesetPath).append("</RuleSet>\n")
          .append(" </Payload>\n")
          .append("</Evento>");
        return sb.toString();
    }

    /* ===================== Envio para S3 ===================== */
    
    private static void sendToS3(String content, String type, long timestampMs) throws Exception {
        SimpleDateFormat partitionFormat = new SimpleDateFormat("yyyy/MM/dd/HH");
        String partition = partitionFormat.format(new Date(timestampMs));
        
        String finalPrefix = PREFIX;
        if (finalPrefix != null && !finalPrefix.isEmpty() && !finalPrefix.endsWith("/")) {
            finalPrefix += "/";
        } else if (finalPrefix == null || finalPrefix.isEmpty()) {
            finalPrefix = "odm-metrics/";
        }
        
        String key = finalPrefix + partition + "/" + type + "-report-" + timestampMs + ".xml";
        
        byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
        InputStream inputStream = new ByteArrayInputStream(contentBytes);
        
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(contentBytes.length);
        metadata.setContentType("application/xml");
        
        PutObjectRequest request = new PutObjectRequest(BUCKET_NAME, key, inputStream, metadata);
        S3_CLIENT.putObject(request);
        
        System.out.println("[ODM-S3] Arquivo enviado: s3://" + BUCKET_NAME + "/" + key);
    }
}

// Made with Bob
