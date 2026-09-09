package br.com.itau.odm.embarcado;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Testes unitários para S3Metrics.
 *
 * Testamos apenas comportamentos que não requerem conexão real com S3:
 *   - recordExecution: acumula contadores corretamente
 *   - updateRuleset: atualiza o caminho do ruleset
 *   - getMetricsAsJson: retorna JSON bem formado com valores corretos
 *   - getTotalCount: retorna o total acumulado
 *   - validateOrThrow / close: são seguros quando S3 não está configurado
 *
 * Cada teste reseta o estado estático via reflection para garantir isolamento.
 */
@DisplayName("S3Metrics")
class S3MetricsTest {

    @AfterEach
    void resetS3MetricsState() throws Exception {
        // Resetar todos os campos estáticos para estado inicial
        setLong("TOTAL_COUNT",        0L);
        setLong("OK_COUNT",           0L);
        setLong("ERROR_COUNT",        0L);
        setLong("TOTAL_DURATION_MS",  0L);
        setLong("TOTAL_RULES_FIRED",  0L);
        setLong("START_TS_MS",        0L);
        setLong("END_TS_MS",          0L);
        setLong("LAST_FLUSH_COUNT",   0L);
        setBool("SUMMARY_SENT",       false);
        setBool("INIT",               false);
        setBool("CLOSED",             false);
        setVolatile("S3_READY",       false);
        setVolatile("RULESET_PATH",   "(unknown)");
    }

    // -------------------------------------------------------------------------
    // getTotalCount
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("getTotalCount() retorna 0 em estado inicial")
    void getTotalCount_initial_returnsZero() {
        assertEquals(0L, S3Metrics.getTotalCount());
    }

    @Test
    @DisplayName("getTotalCount() reflete execuções registradas")
    void getTotalCount_afterRecords_returnsCorrectCount() {
        S3Metrics.recordExecution("/ruleset/1.0/test", 100L, 5, true);
        S3Metrics.recordExecution("/ruleset/1.0/test", 200L, 8, true);
        assertEquals(2L, S3Metrics.getTotalCount());
    }

    // -------------------------------------------------------------------------
    // recordExecution — acumulação de contadores
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("recordExecution(success=true) incrementa okCount")
    void recordExecution_success_incrementsOkCount() throws Exception {
        S3Metrics.recordExecution("/ruleset/1.0/test", 100L, 5, true);

        assertEquals(1L, getLong("TOTAL_COUNT"));
        assertEquals(1L, getLong("OK_COUNT"));
        assertEquals(0L, getLong("ERROR_COUNT"));
        assertEquals(100L, getLong("TOTAL_DURATION_MS"));
        assertEquals(5L, getLong("TOTAL_RULES_FIRED"));
    }

    @Test
    @DisplayName("recordExecution(success=false) incrementa errorCount")
    void recordExecution_failure_incrementsErrorCount() throws Exception {
        S3Metrics.recordExecution("/ruleset/1.0/test", 50L, 0, false);

        assertEquals(1L, getLong("TOTAL_COUNT"));
        assertEquals(0L, getLong("OK_COUNT"));
        assertEquals(1L, getLong("ERROR_COUNT"));
    }

    @Test
    @DisplayName("recordExecution acumula duração e regras em múltiplas chamadas")
    void recordExecution_multiple_accumulatesDurationAndRules() throws Exception {
        S3Metrics.recordExecution("/ruleset/1.0/test", 100L, 10, true);
        S3Metrics.recordExecution("/ruleset/1.0/test", 200L, 20, true);
        S3Metrics.recordExecution("/ruleset/1.0/test",  50L,  5, false);

        assertEquals(3L,   getLong("TOTAL_COUNT"));
        assertEquals(2L,   getLong("OK_COUNT"));
        assertEquals(1L,   getLong("ERROR_COUNT"));
        assertEquals(350L, getLong("TOTAL_DURATION_MS"));
        assertEquals(35L,  getLong("TOTAL_RULES_FIRED"));
    }

    @Test
    @DisplayName("recordExecution define START_TS_MS na primeira chamada e não sobrescreve")
    void recordExecution_firstCall_setsStartTimestamp() throws Exception {
        S3Metrics.recordExecution("/ruleset/1.0/test", 10L, 1, true);
        long firstStart = getLong("START_TS_MS");
        assertTrue(firstStart > 0L);

        // Segunda chamada não deve sobrescrever o start
        S3Metrics.recordExecution("/ruleset/1.0/test", 10L, 1, true);
        assertEquals(firstStart, getLong("START_TS_MS"),
                "START_TS_MS não deve ser sobrescrito após a primeira execução");
    }

    @Test
    @DisplayName("recordExecution atualiza END_TS_MS a cada chamada")
    void recordExecution_updatesEndTimestamp() throws Exception {
        S3Metrics.recordExecution("/ruleset/1.0/test", 10L, 1, true);
        long first = getLong("END_TS_MS");

        Thread.sleep(2); // garantir diferença de timestamp
        S3Metrics.recordExecution("/ruleset/1.0/test", 10L, 1, true);
        long second = getLong("END_TS_MS");

        assertTrue(second >= first, "END_TS_MS deve ser atualizado");
    }

    // -------------------------------------------------------------------------
    // updateRuleset
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("updateRuleset() atualiza o RULESET_PATH")
    void updateRuleset_updatesPath() throws Exception {
        S3Metrics.updateRuleset("/new/ruleset/path");
        Object rulesetPath = getVolatile("RULESET_PATH");
        assertEquals("/new/ruleset/path", rulesetPath);
    }

    @Test
    @DisplayName("updateRuleset() ignora null")
    void updateRuleset_ignoresNull() throws Exception {
        S3Metrics.updateRuleset("/initial/path");
        S3Metrics.updateRuleset(null);
        assertEquals("/initial/path", getVolatile("RULESET_PATH"));
    }

    @Test
    @DisplayName("updateRuleset() ignora string em branco")
    void updateRuleset_ignoresBlank() throws Exception {
        S3Metrics.updateRuleset("/initial/path");
        S3Metrics.updateRuleset("   ");
        assertEquals("/initial/path", getVolatile("RULESET_PATH"));
    }

    // -------------------------------------------------------------------------
    // recordExecution — sobrecarga de compatibilidade
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("recordExecution(status, durationMs, rulesFired) com 'ok' incrementa okCount")
    void recordExecution_statusOk_incrementsOkCount() throws Exception {
        S3Metrics.recordExecution("ok", 100L, 5);
        assertEquals(1L, getLong("OK_COUNT"));
        assertEquals(0L, getLong("ERROR_COUNT"));
    }

    @Test
    @DisplayName("recordExecution(status, durationMs, rulesFired) com status != 'ok' incrementa errorCount")
    void recordExecution_statusError_incrementsErrorCount() throws Exception {
        S3Metrics.recordExecution("error", 100L, 0);
        assertEquals(0L, getLong("OK_COUNT"));
        assertEquals(1L, getLong("ERROR_COUNT"));
    }

    // -------------------------------------------------------------------------
    // getMetricsAsJson
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("getMetricsAsJson() retorna JSON com campos obrigatórios")
    void getMetricsAsJson_containsRequiredFields() {
        S3Metrics.recordExecution("/ruleset/1.0/test", 100L, 5, true);
        S3Metrics.recordExecution("/ruleset/1.0/test", 200L, 8, false);

        String json = S3Metrics.getMetricsAsJson();

        assertNotNull(json);
        assertTrue(json.contains("\"totalExecutions\""),    "deve conter totalExecutions");
        assertTrue(json.contains("\"successCount\""),       "deve conter successCount");
        assertTrue(json.contains("\"errorCount\""),         "deve conter errorCount");
        assertTrue(json.contains("\"totalDurationMs\""),    "deve conter totalDurationMs");
        assertTrue(json.contains("\"avgDurationMs\""),      "deve conter avgDurationMs");
        assertTrue(json.contains("\"totalRulesFired\""),    "deve conter totalRulesFired");
        assertTrue(json.contains("\"rulesetPath\""),        "deve conter rulesetPath");
        assertTrue(json.contains("\"s3Ready\""),            "deve conter s3Ready");
    }

    @Test
    @DisplayName("getMetricsAsJson() retorna valores corretos")
    void getMetricsAsJson_returnsCorrectValues() {
        S3Metrics.recordExecution("/ruleset/1.0/test", 100L, 5, true);
        S3Metrics.recordExecution("/ruleset/1.0/test", 300L, 10, true);

        String json = S3Metrics.getMetricsAsJson();

        assertTrue(json.contains("\"totalExecutions\": 2"),  "totalExecutions deve ser 2");
        assertTrue(json.contains("\"successCount\": 2"),     "successCount deve ser 2");
        assertTrue(json.contains("\"errorCount\": 0"),       "errorCount deve ser 0");
        assertTrue(json.contains("\"totalDurationMs\": 400"),"totalDurationMs deve ser 400");
        assertTrue(json.contains("\"avgDurationMs\": 200"),  "avgDurationMs deve ser 200");
        assertTrue(json.contains("\"totalRulesFired\": 15"), "totalRulesFired deve ser 15");
    }

    @Test
    @DisplayName("getMetricsAsJson() com zero execuções retorna avgDurationMs = 0")
    void getMetricsAsJson_noExecutions_avgIsZero() {
        String json = S3Metrics.getMetricsAsJson();
        assertTrue(json.contains("\"totalExecutions\": 0"));
        assertTrue(json.contains("\"avgDurationMs\": 0"));
    }

    // -------------------------------------------------------------------------
    // isReady / validateOrThrow / close — seguros sem S3 configurado
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("isReady() retorna false sem inicialização")
    void isReady_withoutInit_returnsFalse() {
        assertFalse(S3Metrics.isReady());
    }

    @Test
    @DisplayName("validateOrThrow() não lança exceção quando S3 não configurado")
    void validateOrThrow_withoutS3_doesNotThrow() {
        assertDoesNotThrow(S3Metrics::validateOrThrow);
    }

    @Test
    @DisplayName("close() não lança exceção quando S3 não configurado")
    void close_withoutS3_doesNotThrow() {
        assertDoesNotThrow(S3Metrics::close);
    }

    // -------------------------------------------------------------------------
    // Helpers de reflection
    // -------------------------------------------------------------------------

    private long getLong(String fieldName) throws Exception {
        Field f = S3Metrics.class.getDeclaredField(fieldName);
        f.setAccessible(true);
        return ((AtomicLong) f.get(null)).get();
    }

    private void setLong(String fieldName, long value) throws Exception {
        Field f = S3Metrics.class.getDeclaredField(fieldName);
        f.setAccessible(true);
        ((AtomicLong) f.get(null)).set(value);
    }

    private void setBool(String fieldName, boolean value) throws Exception {
        Field f = S3Metrics.class.getDeclaredField(fieldName);
        f.setAccessible(true);
        ((AtomicBoolean) f.get(null)).set(value);
    }

    private void setVolatile(String fieldName, Object value) throws Exception {
        Field f = S3Metrics.class.getDeclaredField(fieldName);
        f.setAccessible(true);
        f.set(null, value);
    }

    private Object getVolatile(String fieldName) throws Exception {
        Field f = S3Metrics.class.getDeclaredField(fieldName);
        f.setAccessible(true);
        return f.get(null);
    }
}
