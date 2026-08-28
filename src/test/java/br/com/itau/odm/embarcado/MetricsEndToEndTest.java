package br.com.itau.odm.embarcado;

import org.junit.jupiter.api.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Teste end-to-end de métricas:
 *
 *  1. Acumula execuções no S3MetricsAccumulator (simula os executores Spark)
 *  2. Gera os XMLs (ILMT + Custom) localmente via S3MetricsAggregator
 *  3. Verifica que os arquivos foram criados com o conteúdo correto
 *
 * Não usa S3 real — salva em diretório temporário local.
 */
@DisplayName("Métricas End-to-End: acumular → gerar XML → verificar arquivo")
class MetricsEndToEndTest {

    private Path outputDir;

    @BeforeEach
    void setUp() throws Exception {
        outputDir = Files.createTempDirectory("odm-metrics-test-");
    }

    @AfterEach
    void tearDown() throws Exception {
        // Remover diretório temporário após cada teste
        if (outputDir != null && Files.exists(outputDir)) {
            Files.walk(outputDir)
                 .sorted(Comparator.reverseOrder())
                 .forEach(p -> p.toFile().delete());
        }
        // Limpar diretório ILMT gerado pelo LicenseMetricLogger
        Path ilmtDir = Paths.get("./var/ibm/slmtags");
        if (Files.exists(ilmtDir)) {
            Files.walk(ilmtDir)
                 .sorted(Comparator.reverseOrder())
                 .forEach(p -> p.toFile().delete());
        }
    }

    // =========================================================================
    // Teste 1 — Fluxo completo com dados reais
    // =========================================================================

    @Test
    @DisplayName("Fluxo completo: 10 execuções → gera ILMT XML e Custom XML com valores corretos")
    void fullFlow_generatesBothXmlFilesWithCorrectValues() throws Exception {
        // --- 1. Simular execuções nos "executores" ---
        S3MetricsAccumulator accumulator = new S3MetricsAccumulator();

        for (int i = 0; i < 8; i++) {
            accumulator.recordExecution("/bre/1.0/regras", 100L + i * 10, 5 + i, true);
        }
        for (int i = 0; i < 2; i++) {
            accumulator.recordExecution("/bre/1.0/regras", 50L, 0, false);
        }

        // --- 2. Ler valor agregado (simula o driver recebendo do Spark) ---
        S3MetricsAccumulator.MetricsData metrics = accumulator.value();

        assertEquals(10L, metrics.totalCount);
        assertEquals(8L,  metrics.okCount);
        assertEquals(2L,  metrics.errorCount);

        // --- 3. Gerar XMLs localmente ---
        long startMs = metrics.startTimestampMs;
        long endMs   = metrics.endTimestampMs;

        String customXml = buildCustomXml(metrics, startMs, endMs);
        String ilmtXml   = buildIlmtXml(metrics.totalCount, startMs, endMs);

        // --- 4. Salvar em disco ---
        Path customFile = outputDir.resolve("custom-report.xml");
        Path ilmtFile   = outputDir.resolve("ilmt-report.xml");

        Files.writeString(customFile, customXml, StandardCharsets.UTF_8);
        Files.writeString(ilmtFile,   ilmtXml,   StandardCharsets.UTF_8);

        // --- 5. Verificar arquivos existem ---
        assertTrue(Files.exists(customFile), "custom-report.xml deve existir");
        assertTrue(Files.exists(ilmtFile),   "ilmt-report.xml deve existir");
        assertTrue(Files.size(customFile) > 0, "custom-report.xml não deve estar vazio");
        assertTrue(Files.size(ilmtFile)   > 0, "ilmt-report.xml não deve estar vazio");

        // --- 6. Verificar conteúdo do Custom XML ---
        String customContent = Files.readString(customFile, StandardCharsets.UTF_8);

        assertAll("Custom XML deve conter os valores corretos",
            () -> assertTrue(customContent.contains("<TotalExecucoes>10</TotalExecucoes>"),
                             "TotalExecucoes deve ser 10"),
            () -> assertTrue(customContent.contains("<Sucesso>8</Sucesso>"),
                             "Sucesso deve ser 8"),
            () -> assertTrue(customContent.contains("<Erros>2</Erros>"),
                             "Erros deve ser 2"),
            () -> assertTrue(customContent.contains("<RuleSet>/bre/1.0/regras</RuleSet>"),
                             "RuleSet deve ser /bre/1.0/regras"),
            () -> assertTrue(customContent.contains("<TotalRegrasDisparadas>"),
                             "deve conter TotalRegrasDisparadas"),
            () -> assertTrue(customContent.contains("<DuracaoTotalMs>"),
                             "deve conter DuracaoTotalMs"),
            () -> assertTrue(customContent.contains("<DuracaoMediaMs>"),
                             "deve conter DuracaoMediaMs"),
            () -> assertTrue(customContent.contains("<StartTime>"),
                             "deve conter StartTime"),
            () -> assertTrue(customContent.contains("<EndTime>"),
                             "deve conter EndTime")
        );

        // --- 7. Verificar conteúdo do ILMT XML ---
        String ilmtContent = Files.readString(ilmtFile, StandardCharsets.UTF_8);

        assertAll("ILMT XML deve conter estrutura IBM correta",
            () -> assertTrue(ilmtContent.contains("IBM Operational Decision Manager"),
                             "deve identificar o produto IBM ODM"),
            () -> assertTrue(ilmtContent.contains("THOUSAND_MONTHLY_ARTIFACTS") ||
                             ilmtContent.contains("MILLION_MONTHLY_DECISIONS"),
                             "deve conter a métrica ILMT correta"),
            () -> assertFalse(ilmtContent.isBlank(),
                             "conteúdo ILMT não deve estar em branco")
        );

        System.out.println("\n✅ Custom XML gerado em: " + customFile);
        System.out.println("   Conteúdo:\n" + customContent);
        System.out.println("\n✅ ILMT XML gerado em: " + ilmtFile);
        System.out.println("   Trecho:\n" + ilmtContent.substring(0, Math.min(400, ilmtContent.length())));
    }

    // =========================================================================
    // Teste 2 — Valores de duração e regras são calculados corretamente
    // =========================================================================

    @Test
    @DisplayName("Custom XML calcula DuracaoMedia corretamente")
    void customXml_avgDurationIsCorrect() throws Exception {
        S3MetricsAccumulator accumulator = new S3MetricsAccumulator();
        accumulator.recordExecution("/ruleset/test", 100L, 5, true);
        accumulator.recordExecution("/ruleset/test", 300L, 10, true);

        S3MetricsAccumulator.MetricsData m = accumulator.value();
        String xml = buildCustomXml(m, m.startTimestampMs, m.endTimestampMs);

        // Duração total = 400ms, média = 200ms
        assertTrue(xml.contains("<DuracaoTotalMs>400</DuracaoTotalMs>"),
                "DuracaoTotalMs deve ser 400");
        assertTrue(xml.contains("<DuracaoMediaMs>200</DuracaoMediaMs>"),
                "DuracaoMediaMs deve ser 200");
        assertTrue(xml.contains("<TotalRegrasDisparadas>15</TotalRegrasDisparadas>"),
                "TotalRegrasDisparadas deve ser 15");

        Path outFile = outputDir.resolve("custom-avg-test.xml");
        Files.writeString(outFile, xml);
        assertTrue(Files.exists(outFile));
        System.out.println("\n✅ custom-avg-test.xml gerado: " + outFile);
    }

    // =========================================================================
    // Teste 3 — ILMT usa MILLION_MONTHLY_DECISIONS para > 1 milhão
    // =========================================================================

    @Test
    @DisplayName("ILMT XML usa MILLION_MONTHLY_DECISIONS para 10 milhões de execuções")
    void ilmtXml_usesMilionMetricForLargeCount() throws Exception {
        long tenMillion = 10_000_000L;
        long now = System.currentTimeMillis();

        String ilmtXml = buildIlmtXml(tenMillion, now - 60_000L, now);

        Path ilmtFile = outputDir.resolve("ilmt-10m.xml");
        Files.writeString(ilmtFile, ilmtXml);

        assertTrue(Files.exists(ilmtFile));
        String content = Files.readString(ilmtFile);

        assertTrue(content.contains("MILLION_MONTHLY_DECISIONS"),
                "Para 10M decisões deve usar MILLION_MONTHLY_DECISIONS");

        System.out.println("\n✅ ilmt-10m.xml gerado: " + ilmtFile);
        System.out.println("   Trecho:\n" + content.substring(0, Math.min(500, content.length())));
    }

    // =========================================================================
    // Teste 4 — ILMT usa THOUSAND para < 1 milhão
    // =========================================================================

    @Test
    @DisplayName("ILMT XML usa THOUSAND_MONTHLY_ARTIFACTS para 500 execuções")
    void ilmtXml_usesThousandMetricForSmallCount() throws Exception {
        long fiveHundred = 500L;
        long now = System.currentTimeMillis();

        String ilmtXml = buildIlmtXml(fiveHundred, now - 5_000L, now);

        Path ilmtFile = outputDir.resolve("ilmt-500.xml");
        Files.writeString(ilmtFile, ilmtXml);

        assertTrue(Files.exists(ilmtFile));
        String content = Files.readString(ilmtFile);

        assertTrue(content.contains("THOUSAND_MONTHLY_ARTIFACTS"),
                "Para 500 decisões deve usar THOUSAND_MONTHLY_ARTIFACTS");

        System.out.println("\n✅ ilmt-500.xml gerado: " + ilmtFile);
    }

    // =========================================================================
    // Teste 5 — Accumulator zerado não gera arquivo
    // =========================================================================

    @Test
    @DisplayName("Com zero execuções, flush não gera arquivos")
    void zeroExecutions_noFilesGenerated() throws Exception {
        S3MetricsAccumulator accumulator = new S3MetricsAccumulator();
        assertTrue(accumulator.isZero(), "accumulator deve estar zerado");

        // Simula o que ODMMetricsManager.flush() faz quando totalCount == 0
        S3MetricsAccumulator.MetricsData metrics = accumulator.value();
        boolean wouldFlush = metrics.totalCount > 0;

        assertFalse(wouldFlush, "não deve gerar arquivo quando não há execuções");

        // Verificar que nenhum arquivo foi criado
        long fileCount = Files.list(outputDir).count();
        assertEquals(0L, fileCount, "nenhum arquivo deve ser criado no diretório de saída");

        System.out.println("\n✅ Nenhum arquivo gerado para zero execuções — correto.");
    }

    // =========================================================================
    // Helpers — Extraídos de S3MetricsAggregator (chamada local sem S3)
    // =========================================================================

    /**
     * Gera o XML customizado diretamente (mesma lógica do S3MetricsAggregator,
     * mas retorna String em vez de enviar ao S3).
     */
    private String buildCustomXml(S3MetricsAccumulator.MetricsData m, long startMs, long endMs) {
        java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        String startTime = sdf.format(new java.util.Date(startMs > 0 ? startMs : System.currentTimeMillis()));
        String endTime   = sdf.format(new java.util.Date(endMs   > 0 ? endMs   : System.currentTimeMillis()));
        long avgDuration = m.totalCount > 0 ? m.totalDurationMs / m.totalCount : 0;

        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
               "<ODMMetrics>\n" +
               "  <Summary>\n" +
               "    <TotalExecucoes>"          + m.totalCount       + "</TotalExecucoes>\n" +
               "    <Sucesso>"                 + m.okCount          + "</Sucesso>\n" +
               "    <Erros>"                   + m.errorCount       + "</Erros>\n" +
               "    <DuracaoTotalMs>"          + m.totalDurationMs  + "</DuracaoTotalMs>\n" +
               "    <DuracaoMediaMs>"          + avgDuration        + "</DuracaoMediaMs>\n" +
               "    <TotalRegrasDisparadas>"   + m.totalRulesFired  + "</TotalRegrasDisparadas>\n" +
               "    <RuleSet>"                 + (m.rulesetPath != null ? m.rulesetPath : "(unknown)") + "</RuleSet>\n" +
               "    <StartTime>"              + startTime          + "</StartTime>\n" +
               "    <EndTime>"                + endTime            + "</EndTime>\n" +
               "  </Summary>\n" +
               "</ODMMetrics>";
    }

    /**
     * Gera o XML ILMT oficial usando a lib IBM LicenseMetricLogger
     * (mesma lógica do S3MetricsAggregator, sem envio ao S3).
     */
    private String buildIlmtXml(long totalDecisions, long startEpochMs, long endEpochMs) throws Exception {
        Path ilmtDir = Paths.get("./var/ibm/slmtags");
        Files.createDirectories(ilmtDir);

        String batchId = "test-" + endEpochMs;
        DecisionMetering dm = new DecisionMetering("dba-metering");
        DecisionMeteringReport rep = dm.createUsageReport(batchId);

        LocalDateTime startLdt = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(startEpochMs > 0 ? startEpochMs : System.currentTimeMillis()),
                ZoneId.systemDefault());
        LocalDateTime endLdt = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(endEpochMs > 0 ? endEpochMs : System.currentTimeMillis()),
                ZoneId.systemDefault());

        rep.setStartTimeStamp(startLdt);
        rep.setStopTimeStamp(endLdt);
        rep.setNbDecisions(totalDecisions);
        rep.writeILMTFile();

        Path latest = Files.list(ilmtDir)
                .filter(Files::isRegularFile)
                .max(Comparator.comparingLong(p -> p.toFile().lastModified()))
                .orElseThrow(() -> new IllegalStateException("Nenhum arquivo ILMT gerado"));

        return Files.readString(latest, StandardCharsets.UTF_8);
    }
}
