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
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * S3Metrics - Envia métricas ILMT do ODM automaticamente para S3
 * 
 * Funcionalidades:
 * - Acumula execuções ODM (contadores thread-safe)
 * - Gera relatórios ILMT (XML) no shutdown
 * - Particionamento por data (yyyy/MM/dd/HH)
 * - Compatível com formato IBM License Metric Tool
 * - Configuração via arquivo s3-config.properties ou variáveis de ambiente
 */
final class S3Metrics {
    
    /* ===== Estado de ciclo de vida ===== */
    private static final AtomicBoolean INIT = new AtomicBoolean(false);
    private static final AtomicBoolean HOOKED = new AtomicBoolean(false);
    private static final AtomicBoolean CLOSED = new AtomicBoolean(false);
    private static final AtomicBoolean SUMMARY_SENT = new AtomicBoolean(false);
    private static volatile boolean S3_READY = false;
    
    /* ===== Cliente S3 ===== */
    private static AmazonS3 S3_CLIENT;
    private static String BUCKET_NAME;
    private static String BASE_PREFIX = "odm-metrics/"; // default
    private static String REGION = "us-east-1"; // default
    
    /* ===== Acumuladores de métricas ILMT (thread-safe) ===== */
    private static final AtomicLong TOTAL_COUNT = new AtomicLong(0);
    private static final AtomicLong OK_COUNT = new AtomicLong(0);
    private static final AtomicLong ERROR_COUNT = new AtomicLong(0);
    private static final AtomicLong TOTAL_DURATION_MS = new AtomicLong(0);
    private static final AtomicLong TOTAL_RULES_FIRED = new AtomicLong(0);
    
    /* ===== Janela de tempo da execução agregada ===== */
    private static final AtomicLong START_TS_MS = new AtomicLong(0L);
    private static final AtomicLong END_TS_MS = new AtomicLong(0L);
    
    /* ===== Ruleset path ===== */
    private static volatile String RULESET_PATH = "unknown";
    
    /* ===== Controle de erro ===== */
    private static volatile String LAST_ERROR_MSG = null;
    
    /* ===== Estatísticas ===== */
    private static final AtomicLong TOTAL_SENT = new AtomicLong(0);
    private static final AtomicLong TOTAL_ERRORS = new AtomicLong(0);
    
    private S3Metrics() {}
    
    /* ===================== API pública ===================== */
    
    /**
     * Registra uma execução ODM (acumula contadores para relatório ILMT).
     * Compatível com KafkaMetrics.
     */
    static void recordExecution(String rulesetPath, long durationMs, int rulesFired, boolean success) {
        initIfNeeded();
        
        if (!S3_READY) {
            return; // Silenciosamente ignora se S3 não estiver configurado
        }
        
        try {
            // Atualizar ruleset path
            if (rulesetPath != null && !rulesetPath.isEmpty()) {
                RULESET_PATH = rulesetPath;
            }
            
            // Atualizar janela de tempo
            long now = System.currentTimeMillis();
            START_TS_MS.compareAndSet(0L, now);
            END_TS_MS.set(now);
            
            // Incrementar contadores
            TOTAL_COUNT.incrementAndGet();
            if (success) {
                OK_COUNT.incrementAndGet();
            } else {
                ERROR_COUNT.incrementAndGet();
            }
            TOTAL_DURATION_MS.addAndGet(durationMs);
            TOTAL_RULES_FIRED.addAndGet(rulesFired);
            
        } catch (Exception e) {
            LAST_ERROR_MSG = "Erro ao registrar execução: " + e.getMessage();
            TOTAL_ERRORS.incrementAndGet();
        }
    }
    
    /**
     * Envia o relatório ILMT final para S3 (chamado no shutdown).
     */
    static void flushSummary() {
        if (!SUMMARY_SENT.compareAndSet(false, true)) {
            return; // Já enviado
        }
        
        if (CLOSED.get()) {
            System.err.println("[S3-METRICS] Cliente S3 já fechado - resumo NÃO enviado");
            return;
        }
        
        initIfNeeded();
        if (!S3_READY) {
            System.err.println("[S3-METRICS] S3 não configurado - resumo NÃO enviado (contagem local: " + TOTAL_COUNT.get() + ")");
            return;
        }
        
        try {
            // Snapshot dos contadores
            final long totalCount = TOTAL_COUNT.get();
            final long okCount = OK_COUNT.get();
            final long errorCount = ERROR_COUNT.get();
            final long totalDuration = TOTAL_DURATION_MS.get();
            final long totalRulesFired = TOTAL_RULES_FIRED.get();
            final long avgDuration = (totalCount == 0 ? 0 : totalDuration / totalCount);
            final long startMs = (START_TS_MS.get() == 0 ? System.currentTimeMillis() : START_TS_MS.get());
            final long endMs = (END_TS_MS.get() == 0 ? System.currentTimeMillis() : END_TS_MS.get());
            
            System.out.printf("[S3-METRICS] Snapshot resumo: total=%d ok=%d err=%d durMs=%d avg=%d rules=%d ruleset=%s%n",
                    totalCount, okCount, errorCount, totalDuration, avgDuration, totalRulesFired, RULESET_PATH);
            
            if (totalCount == 0) {
                System.out.println("[S3-METRICS] Nenhuma execução registrada - resumo NÃO enviado");
                return;
            }
            
            // Gerar XML ILMT
            String ilmtXml = buildIlmtXml(totalCount, startMs, endMs);
            
            // Gerar XML customizado
            String customXml = buildCustomXml(totalCount, okCount, errorCount, totalDuration, avgDuration, totalRulesFired, startMs, endMs);
            
            // Enviar para S3
            sendToS3("ilmt-report", ilmtXml);
            sendToS3("custom-report", customXml);
            
            System.out.println("[S3-METRICS] Relatórios ILMT enviados para S3 (decisions=" + totalCount + ")");
            TOTAL_SENT.addAndGet(2); // 2 arquivos enviados
            
        } catch (Exception e) {
            LAST_ERROR_MSG = "Erro ao enviar resumo: " + e.getMessage();
            TOTAL_ERRORS.incrementAndGet();
            System.err.println("[S3-METRICS] ERRO ao enviar resumo: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Verifica se S3 está configurado e pronto.
     */
    static boolean isReady() {
        return S3_READY;
    }
    
    /**
     * Inicializa S3Metrics se ainda não foi inicializado.
     */
    static void initIfNeeded() {
        if (INIT.compareAndSet(false, true)) {
            try {
                loadConfiguration();
                
                if (BUCKET_NAME != null && !BUCKET_NAME.isEmpty()) {
                    S3_CLIENT = AmazonS3ClientBuilder.standard()
                            .withRegion(REGION)
                            .withCredentials(new DefaultAWSCredentialsProviderChain())
                            .build();
                    
                    S3_READY = true;
                    System.out.println("[S3-METRICS] Inicializado: bucket=" + BUCKET_NAME + ", prefix=" + BASE_PREFIX + ", region=" + REGION);
                } else {
                    System.out.println("[S3-METRICS] Bucket não configurado - S3 desabilitado");
                }
                
            } catch (Exception e) {
                System.err.println("[S3-METRICS] Erro ao inicializar: " + e.getMessage());
                LAST_ERROR_MSG = e.getMessage();
            }
        }
        
        // Registrar shutdown hook
        registerShutdownHookOnce();
    }
    
    private static void registerShutdownHookOnce() {
        if (HOOKED.compareAndSet(false, true)) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    flushSummary();
                } catch (Exception e) {
                    System.err.println("[S3-METRICS] Erro no shutdown: " + e.getMessage());
                } finally {
                    closeClient();
                }
            }, "s3-metrics-shutdown"));
        }
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
    
    /* ===================== Métodos privados ===================== */
    
    /**
     * Carrega configuração de variáveis de ambiente ou arquivo.
     */
    private static void loadConfiguration() {
        // Tentar variáveis de ambiente primeiro
        BUCKET_NAME = System.getenv("S3_METRICS_BUCKET");
        String prefix = System.getenv("S3_METRICS_PREFIX");
        String region = System.getenv("S3_METRICS_REGION");
        
        if (prefix != null && !prefix.isEmpty()) {
            BASE_PREFIX = prefix.endsWith("/") ? prefix : prefix + "/";
        }
        if (region != null && !region.isEmpty()) {
            REGION = region;
        }
        
        // Se não encontrou nas variáveis de ambiente, tentar arquivo
        if (BUCKET_NAME == null || BUCKET_NAME.isEmpty()) {
            try {
                Properties props = new Properties();
                
                // Tentar carregar de arquivo
                String configPath = System.getProperty("s3.metrics.config", "s3-config.properties");
                if (Files.exists(Paths.get(configPath))) {
                    props.load(Files.newInputStream(Paths.get(configPath)));
                } else {
                    // Tentar classpath
                    InputStream is = S3Metrics.class.getClassLoader().getResourceAsStream("s3-config.properties");
                    if (is != null) {
                        props.load(is);
                        is.close();
                    }
                }
                
                if (!props.isEmpty()) {
                    BUCKET_NAME = props.getProperty("s3.metrics.bucket");
                    String filePref = props.getProperty("s3.metrics.prefix");
                    String fileReg = props.getProperty("s3.metrics.region");
                    
                    if (filePref != null && !filePref.isEmpty()) {
                        BASE_PREFIX = filePref.endsWith("/") ? filePref : filePref + "/";
                    }
                    if (fileReg != null && !fileReg.isEmpty()) {
                        REGION = fileReg;
                    }
                }
            } catch (Exception e) {
                System.err.println("[S3-METRICS] Erro ao carregar configuração: " + e.getMessage());
            }
        }
    }
    
    /**
     * Constrói XML ILMT compatível com IBM License Metric Tool.
     */
    private static String buildIlmtXml(long totalCount, long startMs, long endMs) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        String startTime = sdf.format(new Date(startMs));
        String endTime = sdf.format(new Date(endMs));
        
        StringBuilder xml = new StringBuilder();
        xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        xml.append("<DecisionServiceMetering>\n");
        xml.append("  <RulesetPath>").append(escapeXml(RULESET_PATH)).append("</RulesetPath>\n");
        xml.append("  <StartTime>").append(startTime).append("</StartTime>\n");
        xml.append("  <EndTime>").append(endTime).append("</EndTime>\n");
        xml.append("  <TotalDecisions>").append(totalCount).append("</TotalDecisions>\n");
        xml.append("  <ProductName>IBM Operational Decision Manager</ProductName>\n");
        xml.append("  <ProductVersion>8.12.0</ProductVersion>\n");
        xml.append("</DecisionServiceMetering>\n");
        
        return xml.toString();
    }
    
    /**
     * Constrói XML customizado com estatísticas detalhadas.
     */
    private static String buildCustomXml(long totalCount, long okCount, long errorCount,
                                         long totalDuration, long avgDuration, long totalRulesFired,
                                         long startMs, long endMs) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        String startTime = sdf.format(new Date(startMs));
        String endTime = sdf.format(new Date(endMs));
        
        StringBuilder xml = new StringBuilder();
        xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        xml.append("<ODMExecutionSummary>\n");
        xml.append("  <RulesetPath>").append(escapeXml(RULESET_PATH)).append("</RulesetPath>\n");
        xml.append("  <TimeWindow>\n");
        xml.append("    <Start>").append(startTime).append("</Start>\n");
        xml.append("    <End>").append(endTime).append("</End>\n");
        xml.append("  </TimeWindow>\n");
        xml.append("  <Executions>\n");
        xml.append("    <Total>").append(totalCount).append("</Total>\n");
        xml.append("    <Success>").append(okCount).append("</Success>\n");
        xml.append("    <Errors>").append(errorCount).append("</Errors>\n");
        xml.append("  </Executions>\n");
        xml.append("  <Performance>\n");
        xml.append("    <TotalDurationMs>").append(totalDuration).append("</TotalDurationMs>\n");
        xml.append("    <AverageDurationMs>").append(avgDuration).append("</AverageDurationMs>\n");
        xml.append("    <TotalRulesFired>").append(totalRulesFired).append("</TotalRulesFired>\n");
        xml.append("  </Performance>\n");
        xml.append("</ODMExecutionSummary>\n");
        
        return xml.toString();
    }
    
    /**
     * Envia conteúdo XML para S3 com particionamento por data.
     */
    private static void sendToS3(String filePrefix, String content) throws Exception {
        // Particionamento: yyyy/MM/dd/HH
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd/HH");
        String partition = sdf.format(new Date());
        
        // Nome do arquivo com timestamp
        long timestamp = System.currentTimeMillis();
        String fileName = String.format("%s-%d.xml", filePrefix, timestamp);
        
        // Key completo
        String key = BASE_PREFIX + partition + "/" + fileName;
        
        // Upload para S3
        byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
        InputStream inputStream = new ByteArrayInputStream(contentBytes);
        
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(contentBytes.length);
        metadata.setContentType("application/xml");
        
        PutObjectRequest request = new PutObjectRequest(BUCKET_NAME, key, inputStream, metadata);
        S3_CLIENT.putObject(request);
        
        System.out.println("[S3-METRICS] Arquivo enviado: s3://" + BUCKET_NAME + "/" + key);
    }
    
    /**
     * Escapa caracteres especiais XML.
     */
    private static String escapeXml(String text) {
        if (text == null) return "";
        return text.replace("&", "&")
                   .replace("<", "<")
                   .replace(">", ">")
                   .replace("\"", """)
                   .replace("'", "'");
    }
}

// Made with Bob