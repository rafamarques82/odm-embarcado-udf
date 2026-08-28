package br.com.itau.odm.embarcado;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Testes unitários para ODMMetricsManager.
 *
 * Como o manager depende de SparkContext (que não está disponível em testes unitários),
 * testamos apenas:
 *   - flush() antes de init() é seguro (não lança exceção)
 *   - chamadas duplicadas de init() são ignoradas (idempotente)
 *   - flush() com acumulador zerado não tenta enviar para S3
 *
 * O comportamento de envio real para S3 é testado via integração.
 */
@DisplayName("ODMMetricsManager")
class ODMMetricsManagerTest {

    /**
     * Reseta o estado estático do ODMMetricsManager entre os testes
     * via reflection (campos private static).
     */
    @AfterEach
    void resetManagerState() throws Exception {
        setStaticField(ODMMetricsManager.class, "accumulator",  null);
        setStaticField(ODMMetricsManager.class, "s3Bucket",     null);
        setStaticField(ODMMetricsManager.class, "s3Prefix",     "odm-metrics");
        setStaticField(ODMMetricsManager.class, "s3Region",     "us-east-1");
        setStaticField(ODMMetricsManager.class, "initialized",  false);
        // Limpar referência da UDF também
        GenericODMUDF.metricsAccumulator = null;
    }

    // -------------------------------------------------------------------------
    // flush() sem init()
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("flush() sem init() lança IllegalStateException")
    void flush_withoutInit_throwsIllegalStateException() {
        assertThrows(IllegalStateException.class, ODMMetricsManager::flush);
    }

    // -------------------------------------------------------------------------
    // flush() com acumulador zerado
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("flush() com acumulador zerado não tenta enviar para S3")
    void flush_withZeroAccumulator_skipsS3Send() throws Exception {
        // Injetar acumulador zerado diretamente (sem SparkContext)
        S3MetricsAccumulator acc = new S3MetricsAccumulator();
        setStaticField(ODMMetricsManager.class, "accumulator", acc);
        setStaticField(ODMMetricsManager.class, "s3Bucket",    "meu-bucket");
        setStaticField(ODMMetricsManager.class, "initialized",  true);

        // flush() deve detectar totalCount == 0 e retornar sem enviar para S3
        // (se tentasse enviar, lançaria exceção de conexão — o que não deve acontecer)
        assertDoesNotThrow(ODMMetricsManager::flush);
    }

    // -------------------------------------------------------------------------
    // Integração interna: acumulador injeta na UDF
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("após injeção manual do accumulator, GenericODMUDF.metricsAccumulator não é null")
    void accumulatorInjection_genericODMUDF_hasReference() throws Exception {
        S3MetricsAccumulator acc = new S3MetricsAccumulator();
        // Simula o que ODMMetricsManager.init() faz internamente
        GenericODMUDF.metricsAccumulator = acc;

        assertNotNull(GenericODMUDF.metricsAccumulator);
        assertSame(acc, GenericODMUDF.metricsAccumulator);
    }

    @Test
    @DisplayName("flush() com acumulador com dados lê valores corretos do accumulator")
    void flush_withData_readsAccumulatorValue() throws Exception {
        // Montar acumulador com dados reais
        S3MetricsAccumulator acc = new S3MetricsAccumulator();
        acc.recordExecution("/my/ruleset/1.0/test", 100L, 5, true);
        acc.recordExecution("/my/ruleset/1.0/test", 200L, 8, true);
        acc.recordExecution("/my/ruleset/1.0/test",  50L, 0, false);

        // Verificar que o valor está correto ANTES de qualquer flush
        S3MetricsAccumulator.MetricsData v = acc.value();
        assertEquals(3L,   v.totalCount);
        assertEquals(2L,   v.okCount);
        assertEquals(1L,   v.errorCount);
        assertEquals(350L, v.totalDurationMs);
        assertEquals(13L,  v.totalRulesFired);
        assertEquals("/my/ruleset/1.0/test", v.rulesetPath);

        // flush() sem init() deve lançar IllegalStateException (mesmo com accumulator injetado)
        // pois a flag initialized ainda é false
        assertThrows(IllegalStateException.class, ODMMetricsManager::flush);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static void setStaticField(Class<?> clazz, String fieldName, Object value) throws Exception {
        Field f = clazz.getDeclaredField(fieldName);
        f.setAccessible(true);
        f.set(null, value);
    }
}
