package br.com.itau.odm.embarcado;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Testes unitários para S3MetricsAccumulator.
 * Cobre: acumulação, merge, reset, isZero, copy e casos de borda.
 */
@DisplayName("S3MetricsAccumulator")
class S3MetricsAccumulatorTest {

    private S3MetricsAccumulator accumulator;

    @BeforeEach
    void setUp() {
        accumulator = new S3MetricsAccumulator();
    }

    // -------------------------------------------------------------------------
    // isZero
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("isZero() retorna true quando recém-criado")
    void isZero_newAccumulator_returnsTrue() {
        assertTrue(accumulator.isZero());
    }

    @Test
    @DisplayName("isZero() retorna false após registrar uma execução")
    void isZero_afterRecord_returnsFalse() {
        accumulator.recordExecution("/ruleset/1.0/test", 50L, 3, true);
        assertFalse(accumulator.isZero());
    }

    // -------------------------------------------------------------------------
    // recordExecution — sucesso
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("recordExecution com sucesso incrementa totalCount e okCount")
    void recordExecution_success_incrementsCounters() {
        accumulator.recordExecution("/ruleset/1.0/test", 100L, 5, true);

        S3MetricsAccumulator.MetricsData v = accumulator.value();
        assertEquals(1L, v.totalCount);
        assertEquals(1L, v.okCount);
        assertEquals(0L, v.errorCount);
        assertEquals(100L, v.totalDurationMs);
        assertEquals(5L, v.totalRulesFired);
        assertEquals("/ruleset/1.0/test", v.rulesetPath);
    }

    @Test
    @DisplayName("recordExecution com falha incrementa totalCount e errorCount")
    void recordExecution_failure_incrementsErrorCounter() {
        accumulator.recordExecution("/ruleset/1.0/test", 20L, 0, false);

        S3MetricsAccumulator.MetricsData v = accumulator.value();
        assertEquals(1L, v.totalCount);
        assertEquals(0L, v.okCount);
        assertEquals(1L, v.errorCount);
    }

    @Test
    @DisplayName("múltiplas execuções acumulam corretamente")
    void recordExecution_multiple_accumulatesCorrectly() {
        accumulator.recordExecution("/ruleset/1.0/test", 100L, 10, true);
        accumulator.recordExecution("/ruleset/1.0/test", 200L, 20, true);
        accumulator.recordExecution("/ruleset/1.0/test",  50L,  5, false);

        S3MetricsAccumulator.MetricsData v = accumulator.value();
        assertEquals(3L,   v.totalCount);
        assertEquals(2L,   v.okCount);
        assertEquals(1L,   v.errorCount);
        assertEquals(350L, v.totalDurationMs);
        assertEquals(35L,  v.totalRulesFired);
    }

    // -------------------------------------------------------------------------
    // reset
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("reset() zera todos os contadores")
    void reset_clearsAllCounters() {
        accumulator.recordExecution("/ruleset/1.0/test", 100L, 5, true);
        assertFalse(accumulator.isZero());

        accumulator.reset();

        assertTrue(accumulator.isZero());
        S3MetricsAccumulator.MetricsData v = accumulator.value();
        assertEquals(0L, v.totalCount);
        assertEquals(0L, v.okCount);
        assertEquals(0L, v.errorCount);
        assertEquals(0L, v.totalDurationMs);
        assertEquals(0L, v.totalRulesFired);
    }

    // -------------------------------------------------------------------------
    // copy
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("copy() produz acumulador independente")
    void copy_producesIndependentCopy() {
        accumulator.recordExecution("/ruleset/1.0/test", 100L, 5, true);

        S3MetricsAccumulator copy = (S3MetricsAccumulator) accumulator.copy();

        // Modificar o original não afeta a cópia
        accumulator.recordExecution("/ruleset/1.0/test", 200L, 10, true);

        assertEquals(1L, copy.value().totalCount,
                "cópia não deve refletir mudanças no original após copy()");
        assertEquals(2L, accumulator.value().totalCount);
    }

    // -------------------------------------------------------------------------
    // merge
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("merge() soma contadores de dois acumuladores")
    void merge_sumsCounters() {
        accumulator.recordExecution("/ruleset/1.0/test", 100L, 10, true);

        S3MetricsAccumulator other = new S3MetricsAccumulator();
        other.recordExecution("/ruleset/1.0/test", 200L, 20, false);

        accumulator.merge(other);

        S3MetricsAccumulator.MetricsData v = accumulator.value();
        assertEquals(2L,   v.totalCount);
        assertEquals(1L,   v.okCount);
        assertEquals(1L,   v.errorCount);
        assertEquals(300L, v.totalDurationMs);
        assertEquals(30L,  v.totalRulesFired);
    }

    @Test
    @DisplayName("merge() com acumulador vazio não altera os dados")
    void merge_withEmpty_noChange() {
        accumulator.recordExecution("/ruleset/1.0/test", 100L, 5, true);

        S3MetricsAccumulator empty = new S3MetricsAccumulator();
        accumulator.merge(empty);

        assertEquals(1L, accumulator.value().totalCount);
    }

    // -------------------------------------------------------------------------
    // MetricsData — merge de timestamps e rulesetPath
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("merge mantém o menor startTimestamp e o maior endTimestamp")
    void merge_keepsCorrectTimestampWindow() {
        S3MetricsAccumulator.MetricsData a = new S3MetricsAccumulator.MetricsData();
        a.startTimestampMs = 1000L;
        a.endTimestampMs   = 3000L;

        S3MetricsAccumulator.MetricsData b = new S3MetricsAccumulator.MetricsData();
        b.startTimestampMs = 500L;
        b.endTimestampMs   = 4000L;

        a.merge(b);

        assertEquals(500L,  a.startTimestampMs, "deve manter o menor start");
        assertEquals(4000L, a.endTimestampMs,   "deve manter o maior end");
    }

    @Test
    @DisplayName("merge não sobrescreve rulesetPath conhecido com '(unknown)'")
    void merge_doesNotOverwriteKnownRulesetWithUnknown() {
        S3MetricsAccumulator.MetricsData known = new S3MetricsAccumulator.MetricsData();
        known.rulesetPath = "/my/ruleset";

        S3MetricsAccumulator.MetricsData unknown = new S3MetricsAccumulator.MetricsData();
        // rulesetPath default é "(unknown)"

        known.merge(unknown);

        assertEquals("/my/ruleset", known.rulesetPath);
    }

    @Test
    @DisplayName("merge sobrescreve '(unknown)' com rulesetPath real")
    void merge_overwritesUnknownWithRealRuleset() {
        S3MetricsAccumulator.MetricsData target = new S3MetricsAccumulator.MetricsData();
        // target.rulesetPath == "(unknown)"

        S3MetricsAccumulator.MetricsData source = new S3MetricsAccumulator.MetricsData();
        source.rulesetPath = "/my/ruleset";

        target.merge(source);

        assertEquals("/my/ruleset", target.rulesetPath);
    }

    // -------------------------------------------------------------------------
    // MetricsData.copy
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("MetricsData.copy() produz objeto com os mesmos valores")
    void metricsDataCopy_hasSameValues() {
        S3MetricsAccumulator.MetricsData original = new S3MetricsAccumulator.MetricsData();
        original.totalCount      = 42L;
        original.okCount         = 40L;
        original.errorCount      = 2L;
        original.totalDurationMs = 1000L;
        original.totalRulesFired = 200L;
        original.rulesetPath     = "/my/ruleset";
        original.startTimestampMs = 100L;
        original.endTimestampMs   = 999L;

        S3MetricsAccumulator.MetricsData copy = original.copy();

        assertEquals(original.totalCount,       copy.totalCount);
        assertEquals(original.okCount,          copy.okCount);
        assertEquals(original.errorCount,       copy.errorCount);
        assertEquals(original.totalDurationMs,  copy.totalDurationMs);
        assertEquals(original.totalRulesFired,  copy.totalRulesFired);
        assertEquals(original.rulesetPath,      copy.rulesetPath);
        assertEquals(original.startTimestampMs, copy.startTimestampMs);
        assertEquals(original.endTimestampMs,   copy.endTimestampMs);
    }
}
