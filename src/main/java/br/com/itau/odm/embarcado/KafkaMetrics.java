
package br.com.itau.odm.embarcado;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.net.ssl.SSLHandshakeException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/* ===== Reuso das classes de metering (inalteradas) ===== */
import br.com.itau.odm.embarcado.DecisionMetering;
import br.com.itau.odm.embarcado.DecisionMeteringReport;

/**
 * KafkaMetrics (singleton, thread-safe)
 *
 * - Lê "odm-kafka.properties" (-Dodm.kafka.props / ENV ODM_KAFKA_PROPS / ./ / classpath).
 * - Valida cluster + credenciais + tópico (validateOrThrow()).
 * - Acumula execuções e, no shutdown/close, publica ILMT + XML customizado.
 * - Fail-fast total: em qualquer falha de Kafka, encerra o JVM (System.exit) se configurado.
 */
final class KafkaMetrics {

    /* ===== Estado de ciclo de vida ===== */
    private static final AtomicBoolean INIT   = new AtomicBoolean(false);
    private static final AtomicBoolean HOOKED = new AtomicBoolean(false);
    private static final AtomicBoolean CLOSED = new AtomicBoolean(false);
    private static volatile boolean KAFKA_READY = false;

    /* ===== Kafka ===== */
    private static Producer<String, String> PRODUCER;
    private static String TOPIC;
    private static Properties BASE_PROPS;

    /* ===== Controle de encerramento fatal ===== */
    // Se true, qualquer falha crítica de Kafka chama System.exit(2).
    // Default: false para permitir testes sem Kafka
    private static volatile boolean FATAL_ON_ERROR = false;

    /* ===== Último erro (diagnóstico) ===== */
    // "auth","authz","ssl","topic","conn","config","init","send","health"
    private static volatile String LAST_ERROR_KIND = null;
    private static volatile String LAST_ERROR_MSG  = null;

    /* ===== Acumuladores (thread-safe) ===== */
    private static final AtomicLong TOTAL_COUNT        = new AtomicLong(0);
    private static final AtomicLong OK_COUNT           = new AtomicLong(0);
    private static final AtomicLong ERROR_COUNT        = new AtomicLong(0);
    private static final AtomicLong TOTAL_DURATION_MS  = new AtomicLong(0);
    private static final AtomicLong TOTAL_RULES_FIRED  = new AtomicLong(0);
    private static final AtomicBoolean SUMMARY_SENT    = new AtomicBoolean(false);

    /* ===== Janela de tempo da execução agregada ===== */
    private static final AtomicLong START_TS_MS = new AtomicLong(0L);
    private static final AtomicLong END_TS_MS   = new AtomicLong(0L);

    /* ===== Ruleset dinâmico (atualizado pelo façade) ===== */
    private static volatile String RULESET_PATH = "(unknown)";

    /* ===== Health-check periódico ===== */
    private static final long HEALTH_INTERVAL_MS = 3000L; // ajuste conforme necessidade
    private static final AtomicLong LAST_HEALTH_CHECK_MS = new AtomicLong(0L);

    private KafkaMetrics() {}

    /* ===================== API pública ===================== */

    /** Fail-fast: valida e lança/encerra se cluster/credenciais/tópico não estiverem ok. */
    static void validateOrThrow() {
        initIfNeeded();

        if (!KAFKA_READY) {
            String kind = (LAST_ERROR_KIND != null) ? LAST_ERROR_KIND : "desconhecido";
            String msg  = (LAST_ERROR_MSG  != null) ? LAST_ERROR_MSG  : "Falha não especificada ao validar Kafka.";
            fatalStop(kind, msg, null);
            return; // apenas por completude (fatalStop normalmente dá System.exit)
        }

        // Revalidação leve e periódica (ex.: a cada 3s) para capturar problemas DURANTE a execução
        long now = System.currentTimeMillis();
        long last = LAST_HEALTH_CHECK_MS.get();
        if (now - last >= HEALTH_INTERVAL_MS && LAST_HEALTH_CHECK_MS.compareAndSet(last, now)) {
            revalidateHealthOrStop();
        }
    }

    /** Registra UMA execução; NÃO publica nada agora. */
    static void recordExecution(String status, long durationMs, Integer rulesFired) {
        TOTAL_COUNT.incrementAndGet();
        if ("ok".equalsIgnoreCase(status)) OK_COUNT.incrementAndGet();
        else ERROR_COUNT.incrementAndGet();

        if (durationMs > 0) TOTAL_DURATION_MS.addAndGet(durationMs);
        if (rulesFired != null && rulesFired > 0) TOTAL_RULES_FIRED.addAndGet(rulesFired);

        long now = System.currentTimeMillis();
        START_TS_MS.compareAndSet(0L, now);
        END_TS_MS.set(now);
    }

    /** Atualiza o ruleset dinâmico a partir do façade. */
    static void updateRuleset(String rulesetPath) {
        if (rulesetPath != null && !rulesetPath.isBlank()) RULESET_PATH = rulesetPath;
    }

    /** Fecha: envia o RESUMO (XML ILMT + XML customizado) e fecha o producer (ordem garantida). */
    static void close() { safeFlushAndClose(); }

    /* ===================== Internals ===================== */

    static boolean isReady() { return KAFKA_READY; }

    static void initIfNeeded() {
        if (INIT.get()) return;

        synchronized (KafkaMetrics.class) {
            if (INIT.get()) return;

            try {
                BASE_PROPS = loadKafkaProperties();
                TOPIC = BASE_PROPS.getProperty("topic", "odm.metrics").trim();
                FATAL_ON_ERROR = Boolean.parseBoolean(BASE_PROPS.getProperty("fatal.on.kafka.error", "true"));

                if (TOPIC.isEmpty()) {
                    setError("config", "Propriedade 'topic' ausente ou vazia no arquivo .properties");
                    return;
                }

                // Serializers
                BASE_PROPS.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
                BASE_PROPS.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

                PRODUCER = new KafkaProducer<>(BASE_PROPS);

                // 1) Handshake/credenciais (listTopics força SASL/SSL)
                try (AdminClient admin = AdminClient.create(BASE_PROPS)) {
                    ListTopicsOptions lto = new ListTopicsOptions()
                            .timeoutMs((int) timeoutMs(BASE_PROPS))
                            .listInternal(true);
                    admin.listTopics(lto).names().get(timeoutMs(BASE_PROPS), TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    classifyAndRecordError(e, "Falha na autenticação/handshake ou comunicação com o cluster");
                    return;
                }

                // 2) Tópico (autorização + metadata)
                try (AdminClient admin = AdminClient.create(BASE_PROPS)) {
                    admin.describeTopics(Collections.singletonList(TOPIC))
                         .allTopicNames()
                         .get(timeoutMs(BASE_PROPS), TimeUnit.MILLISECONDS);

                    KAFKA_READY = true;
                    clearError();
                    System.out.println("[ODM-KAFKA] Conexão e tópico validados. Tópico: " + TOPIC);

                } catch (Exception adminEx) {
                    // Fallback: partitionsFor exige permissão de metadata; também valida liderança
                    try {
                        List<PartitionInfo> parts = PRODUCER.partitionsFor(TOPIC);
                        if (parts == null || parts.isEmpty() || parts.get(0).leader() == null) {
                            setError("topic", "Tópico '" + TOPIC + "' não existe/sem partições/sem líder.");
                            return;
                        }
                        KAFKA_READY = true;
                        clearError();
                        System.out.println("[ODM-KAFKA] Conexão validada via partitionsFor(). Tópico: " + TOPIC);
                    } catch (Exception pfEx) {
                        classifyAndRecordError(adminEx, "Falha ao validar o tópico '" + TOPIC + "'");
                    }
                }

                // 3) Sonda de envio síncrona (garante caminho de WRITE/ACKs)
                if (KAFKA_READY) {
                    probeSendOrStop(); // se falhar, encerra o processo
                }

            } catch (Exception e) {
                classifyAndRecordError(e, "Erro ao inicializar KafkaMetrics");
            } finally {
                INIT.set(true);

                // Log de aviso se Kafka não estiver pronto (não bloqueia mais)
                if (!KAFKA_READY) {
                    String kind = (LAST_ERROR_KIND != null) ? LAST_ERROR_KIND : "init";
                    String msg  = (LAST_ERROR_MSG  != null) ? LAST_ERROR_MSG  : "Kafka não validado no boot.";
                    System.err.println("[ODM-KAFKA] AVISO (" + kind + "): " + msg + " - Métricas desabilitadas.");
                }
            }
        }

        // Hook único: envia resumo -> fecha producer
        registerShutdownHookOnce();
    }

    private static void registerShutdownHookOnce() {
        if (HOOKED.compareAndSet(false, true)) {
            Runtime.getRuntime().addShutdownHook(new Thread(KafkaMetrics::safeFlushAndClose, "odm-kafka-shutdown"));
        }
    }

    private static void safeFlushAndClose() {
        try { flushSummaryOnce(); }
        catch (Exception e) { System.err.println("[ODM-KAFKA] Erro ao enviar resumo no shutdown: " + e.getMessage()); }
        finally { closeProducer(); }
    }

    private static void closeProducer() {
        if (CLOSED.compareAndSet(false, true)) {
            try {
                if (PRODUCER != null) {
                    PRODUCER.flush();
                    PRODUCER.close();
                }
            } catch (Exception ignore) {}
        }
    }

    /** Envia o RESUMO uma única vez; não envia se CLOSED=true ou !KAFKA_READY. */
    private static void flushSummaryOnce() {
        if (!SUMMARY_SENT.compareAndSet(false, true)) return; // idempotente
        if (CLOSED.get()) {
            System.err.println("[ODM-KAFKA] Producer já fechado — resumo NÃO enviado (contagem local: "
                    + TOTAL_COUNT.get() + ").");
            return;
        }

        initIfNeeded();
        if (!KAFKA_READY) {
            System.err.println("[ODM-KAFKA] Kafka não validado — resumo NÃO enviado (contagem local: "
                    + TOTAL_COUNT.get() + ").");
            return;
        }

        try {
            // Snapshot único usado nos dois XMLs
            final long totalCount    = TOTAL_COUNT.get();
            final long okCount       = OK_COUNT.get();
            final long errorCount    = ERROR_COUNT.get();
            final long totalDuration = TOTAL_DURATION_MS.get();
            final long avgDuration   = (totalCount == 0 ? 0 : totalDuration / totalCount);
            final long startMs       = (START_TS_MS.get() == 0 ? System.currentTimeMillis() : START_TS_MS.get());
            final long endMs         = (END_TS_MS.get()   == 0 ? System.currentTimeMillis() : END_TS_MS.get());
            final String rulesetSnap = RULESET_PATH;

            System.out.printf("[ODM-KAFKA] Snapshot resumo: total=%d ok=%d err=%d durMs=%d avg=%d ruleset=%s%n",
                    totalCount, okCount, errorCount, totalDuration, avgDuration, rulesetSnap);

            // 1) XML ILMT
            final String ilmtXml = buildIlmtXmlWithDecisionMetering(totalCount, startMs, endMs);
            PRODUCER.send(new ProducerRecord<>(TOPIC, null, ilmtXml), (md, ex) -> {
                if (ex != null) System.err.println("[ODM-KAFKA] Falha ao enviar XML ILMT: " + ex.getMessage());
                else System.out.println("[ODM-KAFKA] XML ILMT enviado ao tópico " + TOPIC + " (decisions=" + totalCount + ")");
            });

            // 2) XML CUSTOMIZADO
            final String customXml = buildCustomXml(
                    totalCount, okCount, errorCount, totalDuration, avgDuration, rulesetSnap, System.currentTimeMillis()
            );
            PRODUCER.send(new ProducerRecord<>(TOPIC, null, customXml), (md2, ex2) -> {
                if (ex2 != null) System.err.println("[ODM-KAFKA] Falha ao enviar XML customizado: " + ex2.getMessage());
                else System.out.println("[ODM-KAFKA] XML customizado enviado ao tópico " + TOPIC);
            });

        } catch (Exception e) {
            System.err.println("[ODM-KAFKA] Erro ao montar/enviar XMLs: " + e.getMessage());
        }
    }

    /* ===================== Health-checks ===================== */

    /** Sonda de envio síncrona (prova de vida do caminho de escrita). */
    private static void probeSendOrStop() {
        try {
            ProducerRecord<String, String> r =
                new ProducerRecord<>(TOPIC, "__odm_probe__", "ping-" + System.currentTimeMillis());
            PRODUCER.send(r).get(timeoutMs(BASE_PROPS), TimeUnit.MILLISECONDS); // força ACK/erros
        } catch (Exception e) {
            classifyAndRecordError(e, "Falha ao enviar 'probe' ao Kafka (write path)");
            fatalStop("send", LAST_ERROR_MSG != null ? LAST_ERROR_MSG : "Erro ao enviar probe", e);
        }
    }

    /** Revalidação leve (executada periodicamente via validateOrThrow). */
    private static void revalidateHealthOrStop() {
        try (AdminClient admin = AdminClient.create(BASE_PROPS)) {
            // 1) Cluster vivo (controller força round-trip)
            admin.describeCluster().controller().get(timeoutMs(BASE_PROPS), TimeUnit.MILLISECONDS);

            // 2) Metadata do tópico consistente
            admin.describeTopics(Collections.singletonList(TOPIC))
                 .allTopicNames()
                 .get(timeoutMs(BASE_PROPS), TimeUnit.MILLISECONDS);

            // 3) Liderança disponível no producer
            List<PartitionInfo> parts = PRODUCER.partitionsFor(TOPIC);
            if (parts == null || parts.isEmpty() || parts.get(0).leader() == null) {
                setError("topic", "Tópico '" + TOPIC + "' ausente/sem líder no health-check.");
                fatalStop("topic", LAST_ERROR_MSG, null);
            }
        } catch (Exception e) {
            classifyAndRecordError(e, "Health-check de Kafka falhou");
            fatalStop("health", LAST_ERROR_MSG != null ? LAST_ERROR_MSG : "Falha no health-check", e);
        }
    }

    /* ===================== Utilitários ===================== */

    private static void setError(String kind, String message) {
        KAFKA_READY = false;
        LAST_ERROR_KIND = kind;
        LAST_ERROR_MSG  = message;
        System.err.println("[ODM-KAFKA] " + kind + ": " + message);
    }

    private static void clearError() {
        LAST_ERROR_KIND = null;
        LAST_ERROR_MSG  = null;
    }

    private static void classifyAndRecordError(Exception e, String ctx) {
        Throwable root = rootCause(e);
        String kind = "conn";
        if (root instanceof SaslAuthenticationException)      kind = "auth";
        else if (root instanceof AuthorizationException)      kind = "authz";
        else if (root instanceof SSLHandshakeException)       kind = "ssl";
        setError(kind, ctx + "\n " + root.getClass().getSimpleName() + ": " + root.getMessage());
    }

    private static Throwable rootCause(Throwable t) {
        Throwable cur = t;
        while (cur.getCause() != null && cur.getCause() != cur) cur = cur.getCause();
        return cur;
    }

    private static long timeoutMs(Properties p) {
        try { return Long.parseLong(p.getProperty("request.timeout.ms", "10000")); }
        catch (Exception ignore) { return 10000L; }
    }

    private static Properties loadKafkaProperties() throws Exception {
        Properties p = new Properties();

        // Defaults (sobrescrevíveis pelo .properties)
        p.setProperty("acks", "all");
        p.setProperty("linger.ms", "5");
        p.setProperty("compression.type", "lz4");
        p.setProperty("delivery.timeout.ms", "120000");
        p.setProperty("max.block.ms", "10000");
        p.setProperty("request.timeout.ms", "10000");
        p.setProperty("enable.idempotence", "true");
        p.setProperty("allow.auto.create.topics", "false");

        // Resolução do arquivo
        String explicitPath = System.getProperty("odm.kafka.props");
        if (explicitPath == null || explicitPath.isBlank()) {
            explicitPath = System.getenv("ODM_KAFKA_PROPS");
        }

        if (explicitPath != null && !explicitPath.isBlank() && Files.exists(Paths.get(explicitPath))) {
            try (InputStream in = Files.newInputStream(Paths.get(explicitPath))) { p.load(in); }
        } else if (Files.exists(Paths.get("odm-kafka.properties"))) {
            try (InputStream in = Files.newInputStream(Paths.get("odm-kafka.properties"))) { p.load(in); }
        } else {
            try (InputStream in = KafkaMetrics.class.getClassLoader().getResourceAsStream("odm-kafka.properties")) {
                if (in != null) p.load(in);
            }
        }

        String bs = p.getProperty("bootstrap.servers");
        if (bs == null || bs.isBlank()) {
            setError("config", "Propriedade obrigatória 'bootstrap.servers' ausente no arquivo .properties");
            throw new IllegalArgumentException("bootstrap.servers ausente");
        }

        if (p.getProperty("topic") == null || p.getProperty("topic").isBlank()) {
            p.setProperty("topic", "odm.metrics");
        }

        // Controle de encerramento fatal (default true)
        if (p.getProperty("fatal.on.kafka.error") == null) {
            p.setProperty("fatal.on.kafka.error", "true");
        }

        // (Opcional) parâmetros de ILMT:
        // p.setProperty("ilmt.dir", "./var/ibm/slmtags");
        // p.setProperty("ilmt.instanceId", "/usr/IBM/TAMIT");

        return p;
    }

    /* ===================== ILMT ===================== */

    /**
     * Gera o slmtag via DecisionMeteringReport.writeILMTFile() e devolve um evento XML:
     * <SchemaVersion> + <SoftwareIdentity> + ÚLTIMO bloco <Metric>...</Metric>.
     */
    private static String buildIlmtXmlWithDecisionMetering(long totalDecisions, long startEpochMs, long endEpochMs) throws Exception {
        Path ilmtDir = Paths.get(getIlmtDir());
        Files.createDirectories(ilmtDir);

        String batchId = "kafka-" + endEpochMs;
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
                .orElseThrow(() -> new IllegalStateException("Nenhum arquivo ILMT encontrado em " + ilmtDir.toAbsolutePath()));

        String content = Files.readString(latest, StandardCharsets.UTF_8);

        String schemaStart   = "<SchemaVersion>";
        String softwareStart = "<SoftwareIdentity>";
        String softwareEnd   = "</SoftwareIdentity>";
        String metricStart   = "<Metric";
        String metricEnd     = "</Metric>";

        int schemaIdx    = content.indexOf(schemaStart);
        int softStartIdx = content.indexOf(softwareStart);
        int softEndIdx   = content.indexOf(softwareEnd) + softwareEnd.length();

        if (schemaIdx < 0 || softStartIdx < 0 || softEndIdx <= softStartIdx) {
            throw new IllegalStateException("Cabeçalho ILMT não encontrado no slmtag.");
        }

        int lastMetricStartIdx = content.lastIndexOf(metricStart);
        if (lastMetricStartIdx < 0) {
            throw new IllegalStateException("Bloco <Metric> não encontrado no slmtag.");
        }

        int lastMetricEndIdx = content.indexOf(metricEnd, lastMetricStartIdx);
        if (lastMetricEndIdx < 0) {
            throw new IllegalStateException("Fechamento </Metric> não encontrado no slmtag.");
        }
        lastMetricEndIdx += metricEnd.length();

        String header     = content.substring(schemaIdx, softEndIdx);
        String lastMetric = content.substring(lastMetricStartIdx, lastMetricEndIdx);

        return header + "\n" + lastMetric;
    }

    private static String getIlmtDir() {
        String prop = (BASE_PROPS != null) ? BASE_PROPS.getProperty("ilmt.dir") : null;
        return (prop == null || prop.isBlank()) ? "./var/ibm/slmtags" : prop;
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
          .append(" <Origem>KafkaMetrics</Origem>\n")
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

    /* ===================== Encerramento fatal ===================== */

    private static void fatalStop(String kind, String message, Exception e) {
        String full = "[ODM-KAFKA] FATAL (" + kind + "): " + message;
        System.err.println(full);
        
        if (FATAL_ON_ERROR) {
            // Fecha producer rapidamente para não travar shutdown
            try { if (PRODUCER != null) PRODUCER.close(); } catch (Exception ignore) {}
            try { System.err.flush(); System.out.flush(); } catch (Exception ignore) {}
            System.exit(2); // encerra o JVM do Spark Driver -> Python cai junto (sem alterar o Python)
        } else {
            // Modo não-fatal: apenas loga o erro e marca Kafka como não disponível
            KAFKA_READY = false;
            System.err.println("[ODM-KAFKA] Kafka indisponível - métricas desabilitadas. Continuando execução...");
        }
    }
}
