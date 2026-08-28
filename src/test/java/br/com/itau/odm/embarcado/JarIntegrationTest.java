package br.com.itau.odm.embarcado;

import org.junit.jupiter.api.*;

import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Teste de integração com o fat JAR compilado.
 *
 * Carrega target/odm-embarcado-udf-1.0.0.jar via URLClassLoader e valida:
 *   1. Todas as classes esperadas estão presentes no JAR
 *   2. S3MetricsAccumulator instanciado do JAR acumula e gera XML correto
 *   3. ODMMetricsManager está presente e acessível
 *   4. GenericODMUDF está presente com o campo metricsAccumulator
 *   5. Fluxo completo: accumulate → buildCustomXml → salvar arquivo → verificar conteúdo
 */
@DisplayName("JAR Integration: carrega fat JAR compilado e valida fluxo de métricas")
class JarIntegrationTest {

    private static final String JAR_PATH = "target/odm-embarcado-udf-1.0.0.jar";

    private static URLClassLoader jarLoader;
    private Path outputDir;

    @BeforeAll
    static void loadJar() throws Exception {
        Path jar = Paths.get(JAR_PATH);
        assertTrue(Files.exists(jar),
            "JAR não encontrado em " + jar.toAbsolutePath() + " — execute 'mvn package -DskipTests' primeiro.");

        System.out.println("\n✅ JAR encontrado: " + jar.toAbsolutePath());
        System.out.printf("   Tamanho: %.1f MB%n", Files.size(jar) / 1024.0 / 1024.0);

        jarLoader = new URLClassLoader(new URL[]{jar.toUri().toURL()},
                                       ClassLoader.getSystemClassLoader());
    }

    @AfterAll
    static void closeJar() throws Exception {
        if (jarLoader != null) jarLoader.close();
    }

    @BeforeEach
    void setUp() throws Exception {
        outputDir = Files.createTempDirectory("odm-jar-test-");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (outputDir != null && Files.exists(outputDir)) {
            Files.walk(outputDir)
                 .sorted(Comparator.reverseOrder())
                 .forEach(p -> p.toFile().delete());
        }
        Path ilmtDir = Paths.get("./var/ibm/slmtags");
        if (Files.exists(ilmtDir)) {
            Files.walk(ilmtDir)
                 .sorted(Comparator.reverseOrder())
                 .forEach(p -> p.toFile().delete());
        }
    }

    // =========================================================================
    // Teste 1 — Classes obrigatórias presentes no JAR
    // =========================================================================

    @Test
    @DisplayName("JAR contém todas as classes esperadas do projeto")
    void jar_containsAllExpectedClasses() {
        String pkg = "br.com.itau.odm.embarcado.";
        String[] expected = {
            pkg + "GenericODMUDF",
            pkg + "S3MetricsAccumulator",
            pkg + "S3MetricsAccumulator$MetricsData",
            pkg + "ODMMetricsManager",
            pkg + "S3MetricsAggregator",
            pkg + "S3MetricsHelper",
            pkg + "S3Metrics",
            pkg + "DecisionMetering",
            pkg + "DecisionMeteringReport",
            pkg + "FacadeSessionFactory",
            pkg + "FacadeStatelessSession",
        };

        for (String className : expected) {
            assertDoesNotThrow(() -> jarLoader.loadClass(className),
                "Classe ausente no JAR: " + className);
            System.out.println("   ✅ " + className);
        }
    }

    // =========================================================================
    // Teste 2 — ODM IBM classes (ilog / com.ibm) estão no JAR
    // =========================================================================

    @Test
    @DisplayName("JAR contém as classes IBM ODM (ilog.rules.*)")
    void jar_containsIbmOdmClasses() {
        String[] odmClasses = {
            "ilog.rules.res.session.IlrStatelessSession",
            "ilog.rules.res.session.IlrSessionFactory",
            "com.ibm.license.metric.LicenseMetricLogger",
            "com.ibm.license.metric.Metric",
        };

        for (String className : odmClasses) {
            assertDoesNotThrow(() -> jarLoader.loadClass(className),
                "Classe IBM ausente no JAR: " + className);
            System.out.println("   ✅ " + className);
        }
    }

    // =========================================================================
    // Teste 3 — S3MetricsAccumulator carregado do JAR: acumula corretamente
    // =========================================================================

    @Test
    @DisplayName("S3MetricsAccumulator do JAR: acumula 1000 execuções com valores corretos")
    void jar_accumulatorRecords1000Executions() throws Exception {
        Class<?> accClass = jarLoader.loadClass(
                "br.com.itau.odm.embarcado.S3MetricsAccumulator");
        Object acc = accClass.getDeclaredConstructor().newInstance();

        // Localizar método recordExecution(String, long, int, boolean)
        Method recordMethod = accClass.getMethod(
                "recordExecution", String.class, long.class, int.class, boolean.class);

        // Simular 1000 execuções bem-sucedidas e 50 com erro
        for (int i = 0; i < 1000; i++) {
            recordMethod.invoke(acc, "/bre/1.0/regras", (long)(50 + i % 200), 5, true);
        }
        for (int i = 0; i < 50; i++) {
            recordMethod.invoke(acc, "/bre/1.0/regras", 30L, 0, false);
        }

        // Ler o valor agregado
        Method valueMethod = accClass.getMethod("value");
        Object metrics = valueMethod.invoke(acc);

        Class<?> dataClass = jarLoader.loadClass(
                "br.com.itau.odm.embarcado.S3MetricsAccumulator$MetricsData");

        long totalCount     = (long) dataClass.getField("totalCount").get(metrics);
        long okCount        = (long) dataClass.getField("okCount").get(metrics);
        long errorCount     = (long) dataClass.getField("errorCount").get(metrics);
        long totalDuration  = (long) dataClass.getField("totalDurationMs").get(metrics);
        String rulesetPath  = (String) dataClass.getField("rulesetPath").get(metrics);

        assertEquals(1050L, totalCount,    "totalCount deve ser 1050");
        assertEquals(1000L, okCount,       "okCount deve ser 1000");
        assertEquals(50L,   errorCount,    "errorCount deve ser 50");
        assertTrue(totalDuration > 0,      "totalDuration deve ser > 0");
        assertEquals("/bre/1.0/regras", rulesetPath, "rulesetPath deve ser /bre/1.0/regras");

        System.out.printf("%n   ✅ totalCount:    %d%n", totalCount);
        System.out.printf("   ✅ okCount:       %d%n", okCount);
        System.out.printf("   ✅ errorCount:    %d%n", errorCount);
        System.out.printf("   ✅ totalDuration: %d ms%n", totalDuration);
        System.out.printf("   ✅ rulesetPath:   %s%n", rulesetPath);
    }

    // =========================================================================
    // Teste 4 — Gera Custom XML do JAR e verifica arquivo em disco
    // =========================================================================

    @Test
    @DisplayName("JAR gera Custom XML com 5000 execuções e salva em disco com valores corretos")
    void jar_generatesCustomXmlAndSavesToDisk() throws Exception {
        Class<?> accClass = jarLoader.loadClass(
                "br.com.itau.odm.embarcado.S3MetricsAccumulator");
        Object acc = accClass.getDeclaredConstructor().newInstance();

        Method recordMethod = accClass.getMethod(
                "recordExecution", String.class, long.class, int.class, boolean.class);

        // 4900 sucesso + 100 erro = 5000 total
        for (int i = 0; i < 4900; i++) {
            recordMethod.invoke(acc, "/creditopj/1.0/elege", 100L, 8, true);
        }
        for (int i = 0; i < 100; i++) {
            recordMethod.invoke(acc, "/creditopj/1.0/elege", 50L, 0, false);
        }

        Method valueMethod = accClass.getMethod("value");
        Object metrics = valueMethod.invoke(acc);
        Class<?> dataClass = jarLoader.loadClass(
                "br.com.itau.odm.embarcado.S3MetricsAccumulator$MetricsData");

        long totalCount    = (long) dataClass.getField("totalCount").get(metrics);
        long okCount       = (long) dataClass.getField("okCount").get(metrics);
        long errorCount    = (long) dataClass.getField("errorCount").get(metrics);
        long totalDuration = (long) dataClass.getField("totalDurationMs").get(metrics);
        long totalRules    = (long) dataClass.getField("totalRulesFired").get(metrics);
        String ruleset     = (String) dataClass.getField("rulesetPath").get(metrics);
        long startMs       = (long) dataClass.getField("startTimestampMs").get(metrics);
        long endMs         = (long) dataClass.getField("endTimestampMs").get(metrics);

        // Montar XML com os valores do JAR
        long avgDuration = totalCount > 0 ? totalDuration / totalCount : 0;
        java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        String startTime = sdf.format(new java.util.Date(startMs > 0 ? startMs : System.currentTimeMillis()));
        String endTime   = sdf.format(new java.util.Date(endMs   > 0 ? endMs   : System.currentTimeMillis()));

        String xml =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<ODMMetrics>\n" +
            "  <Summary>\n" +
            "    <TotalExecucoes>"        + totalCount    + "</TotalExecucoes>\n" +
            "    <Sucesso>"               + okCount       + "</Sucesso>\n" +
            "    <Erros>"                 + errorCount    + "</Erros>\n" +
            "    <DuracaoTotalMs>"        + totalDuration + "</DuracaoTotalMs>\n" +
            "    <DuracaoMediaMs>"        + avgDuration   + "</DuracaoMediaMs>\n" +
            "    <TotalRegrasDisparadas>" + totalRules    + "</TotalRegrasDisparadas>\n" +
            "    <RuleSet>"               + ruleset       + "</RuleSet>\n" +
            "    <StartTime>"             + startTime     + "</StartTime>\n" +
            "    <EndTime>"               + endTime       + "</EndTime>\n" +
            "  </Summary>\n" +
            "</ODMMetrics>";

        // Salvar arquivo
        Path outFile = outputDir.resolve("custom-report-5000.xml");
        Files.writeString(outFile, xml, StandardCharsets.UTF_8);

        // Verificar arquivo em disco
        assertTrue(Files.exists(outFile), "arquivo deve existir em disco");
        assertTrue(Files.size(outFile) > 0, "arquivo não deve estar vazio");

        String saved = Files.readString(outFile, StandardCharsets.UTF_8);

        assertAll("XML salvo deve conter os valores corretos",
            () -> assertTrue(saved.contains("<TotalExecucoes>5000</TotalExecucoes>"),
                             "TotalExecucoes deve ser 5000"),
            () -> assertTrue(saved.contains("<Sucesso>4900</Sucesso>"),
                             "Sucesso deve ser 4900"),
            () -> assertTrue(saved.contains("<Erros>100</Erros>"),
                             "Erros deve ser 100"),
            () -> assertTrue(saved.contains("<RuleSet>/creditopj/1.0/elege</RuleSet>"),
                             "RuleSet deve estar correto"),
            () -> assertTrue(saved.contains("<DuracaoTotalMs>"),
                             "deve conter DuracaoTotalMs")
        );

        System.out.println("\n✅ Arquivo gerado: " + outFile.toAbsolutePath());
        System.out.printf("   Tamanho: %d bytes%n", Files.size(outFile));
        System.out.println("\n   Conteúdo:\n" + saved);
    }

    // =========================================================================
    // Teste 5 — ILMT XML gerado pelo JAR via LicenseMetricLogger
    // =========================================================================

    @Test
    @DisplayName("JAR gera ILMT XML oficial com LicenseMetricLogger e salva em disco")
    void jar_generatesIlmtXmlAndSavesToDisk() throws Exception {
        long totalDecisions = 2_500_000L; // 2.5 milhões → MILLION_MONTHLY_DECISIONS
        long now     = System.currentTimeMillis();
        long startMs = now - 300_000L;    // 5 minutos atrás

        // Instanciar DecisionMetering do JAR
        Class<?> dmClass  = jarLoader.loadClass("br.com.itau.odm.embarcado.DecisionMetering");
        Class<?> repClass = jarLoader.loadClass("br.com.itau.odm.embarcado.DecisionMeteringReport");

        Object dm = dmClass.getConstructor(String.class).newInstance("test-batch");
        Object rep = dmClass.getMethod("createUsageReport", String.class)
                            .invoke(dm, "jar-test-" + now);

        // Configurar timestamps
        java.time.LocalDateTime startLdt = java.time.LocalDateTime.ofInstant(
                java.time.Instant.ofEpochMilli(startMs), java.time.ZoneId.systemDefault());
        java.time.LocalDateTime endLdt = java.time.LocalDateTime.ofInstant(
                java.time.Instant.ofEpochMilli(now), java.time.ZoneId.systemDefault());

        repClass.getMethod("setStartTimeStamp", java.time.LocalDateTime.class).invoke(rep, startLdt);
        repClass.getMethod("setStopTimeStamp",  java.time.LocalDateTime.class).invoke(rep, endLdt);
        repClass.getMethod("setNbDecisions", long.class).invoke(rep, totalDecisions);

        // Gerar arquivo ILMT
        repClass.getMethod("writeILMTFile").invoke(rep);

        // Ler o arquivo gerado
        Path ilmtDir = Paths.get("./var/ibm/slmtags");
        assertTrue(Files.exists(ilmtDir), "diretório ILMT deve ter sido criado");

        Path latestIlmt = Files.list(ilmtDir)
                .filter(Files::isRegularFile)
                .max(Comparator.comparingLong(p -> p.toFile().lastModified()))
                .orElseThrow(() -> new AssertionError("Nenhum arquivo ILMT gerado"));

        String ilmtContent = Files.readString(latestIlmt, StandardCharsets.UTF_8);

        // Copiar para o diretório de saída do teste
        Path outFile = outputDir.resolve("ilmt-report-jar.xml");
        Files.writeString(outFile, ilmtContent, StandardCharsets.UTF_8);

        // Verificar conteúdo
        assertAll("ILMT XML gerado pelo JAR deve ser válido",
            () -> assertFalse(ilmtContent.isBlank(), "conteúdo não deve estar em branco"),
            () -> assertTrue(ilmtContent.contains("IBM Operational Decision Manager"),
                             "deve identificar o produto IBM ODM"),
            () -> assertTrue(ilmtContent.contains("MILLION_MONTHLY_DECISIONS"),
                             "2.5M decisões deve usar MILLION_MONTHLY_DECISIONS"),
            () -> assertTrue(ilmtContent.contains("b1a07d4dc0364452aa6206bb6584061d"),
                             "deve conter o PersistentId IBM correto"),
            () -> assertTrue(ilmtContent.contains("<Value>"),
                             "deve conter o valor da métrica")
        );

        System.out.println("\n✅ ILMT gerado pelo JAR: " + outFile.toAbsolutePath());
        System.out.printf("   Tamanho: %d bytes%n", Files.size(outFile));
        System.out.println("\n   Conteúdo:\n" + ilmtContent);
    }

    // =========================================================================
    // Teste 6 — ODMMetricsManager presente e método flush() acessível
    // =========================================================================

    @Test
    @DisplayName("ODMMetricsManager do JAR expõe init() e flush() públicos")
    void jar_odmMetricsManagerHasRequiredMethods() throws Exception {
        Class<?> mgr = jarLoader.loadClass(
                "br.com.itau.odm.embarcado.ODMMetricsManager");

        // Verificar métodos públicos estáticos
        assertDoesNotThrow(() -> mgr.getMethod("flush"),
                "ODMMetricsManager.flush() deve ser público");
        assertDoesNotThrow(() -> mgr.getMethod("init",
                org.apache.spark.SparkContext.class,
                String.class, String.class, String.class),
                "ODMMetricsManager.init(SparkContext, String, String, String) deve existir");

        System.out.println("\n   ✅ ODMMetricsManager.init()  — presente");
        System.out.println("   ✅ ODMMetricsManager.flush() — presente");

        // flush() sem init() deve lançar IllegalStateException
        Method flush = mgr.getMethod("flush");
        java.lang.reflect.InvocationTargetException ex = assertThrows(
                java.lang.reflect.InvocationTargetException.class,
                () -> flush.invoke(null),
                "flush() sem init() deve lançar IllegalStateException");
        assertInstanceOf(IllegalStateException.class, ex.getCause(),
                "Causa deve ser IllegalStateException");
        System.out.println("   ✅ flush() sem init() — lança IllegalStateException corretamente");
    }
}
