package br.com.itau.odm.embarcado;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Testes unitários para ODMMetricsManager.
 *
 * flush() lê S3_METRICS_BUCKET/PREFIX/REGION das variáveis de ambiente.
 */
@DisplayName("ODMMetricsManager")
class ODMMetricsManagerTest {

    @AfterEach
    void clearEnv() throws Exception {
        // Limpar env via reflection (não há API pública para isso na JVM)
        clearEnvVar("S3_METRICS_BUCKET");
        clearEnvVar("S3_METRICS_PREFIX");
        clearEnvVar("S3_METRICS_REGION");
    }

    // -------------------------------------------------------------------------
    // Validação: bucket ausente lança IllegalArgumentException
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("flush() sem S3_METRICS_BUCKET lança IllegalArgumentException")
    void flush_missingBucket_throwsIllegalArgumentException() {
        // Env var não configurada = bucket null
        assertThrows(IllegalArgumentException.class, () ->
            ODMMetricsManager.flush(
                100L, 99L, 1L, 5000L, "/my/ruleset/1.0/test",
                System.currentTimeMillis() - 5000, System.currentTimeMillis()
            )
        );
    }

    // -------------------------------------------------------------------------
    // totalCount == 0 — retorna sem tentar S3
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("flush() com totalCount=0 não lança exceção e não tenta S3")
    void flush_zeroCount_doesNotThrow() throws Exception {
        setEnvVar("S3_METRICS_BUCKET", "meu-bucket");
        // totalCount=0 → retorna antes de qualquer I/O
        assertDoesNotThrow(() ->
            ODMMetricsManager.flush(
                0L, 0L, 0L, 0L, "/my/ruleset/1.0/test",
                System.currentTimeMillis(), System.currentTimeMillis()
            )
        );
    }

    // -------------------------------------------------------------------------
    // prefix e region nulos usam defaults (sem lançar exceção, totalCount=0)
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("flush() com prefix/region ausentes usa defaults e não lança exceção (totalCount=0)")
    void flush_missingPrefixAndRegion_usesDefaults() throws Exception {
        setEnvVar("S3_METRICS_BUCKET", "meu-bucket");
        // sem PREFIX e REGION → defaults "odm-metrics" e "us-east-1"
        assertDoesNotThrow(() ->
            ODMMetricsManager.flush(
                0L, 0L, 0L, 0L, "/my/ruleset/1.0/test",
                System.currentTimeMillis(), System.currentTimeMillis()
            )
        );
    }

    // -------------------------------------------------------------------------
    // Helpers — manipulação de env vars via reflection (somente em testes)
    // -------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private static void setEnvVar(String name, String value) throws Exception {
        Class<?> pe = Class.forName("java.lang.ProcessEnvironment");
        java.lang.reflect.Field theEnv = pe.getDeclaredField("theEnvironment");
        theEnv.setAccessible(true);
        ((java.util.Map<Object, Object>) theEnv.get(null))
                .put(envKey(name), envVal(value));
    }

    @SuppressWarnings("unchecked")
    private static void clearEnvVar(String name) throws Exception {
        Class<?> pe = Class.forName("java.lang.ProcessEnvironment");
        java.lang.reflect.Field theEnv = pe.getDeclaredField("theEnvironment");
        theEnv.setAccessible(true);
        ((java.util.Map<Object, Object>) theEnv.get(null)).remove(envKey(name));
    }

    private static Object envKey(String s) throws Exception {
        Class<?> c = Class.forName("java.lang.ProcessEnvironment$Variable");
        java.lang.reflect.Method m = c.getDeclaredMethod("valueOf", String.class);
        m.setAccessible(true);
        return m.invoke(null, s);
    }

    private static Object envVal(String s) throws Exception {
        Class<?> c = Class.forName("java.lang.ProcessEnvironment$Value");
        java.lang.reflect.Method m = c.getDeclaredMethod("valueOf", String.class);
        m.setAccessible(true);
        return m.invoke(null, s);
    }
}
