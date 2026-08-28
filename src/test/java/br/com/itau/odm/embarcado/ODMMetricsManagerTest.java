package br.com.itau.odm.embarcado;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Testes unitários para ODMMetricsManager.
 *
 * init() valida e armazena configs; flush() envia usando as configs guardadas.
 */
@DisplayName("ODMMetricsManager")
class ODMMetricsManagerTest {

    @AfterEach
    void resetState() throws Exception {
        setField("s3Bucket", null);
        setField("s3Prefix", "odm-metrics");
        setField("s3Region",  "us-east-1");
    }

    // -------------------------------------------------------------------------
    // init() — validação
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("init() com bucket null lança IllegalArgumentException")
    void init_nullBucket_throws() {
        assertThrows(IllegalArgumentException.class, () ->
            ODMMetricsManager.init(null, null, "prefix", "us-east-1"));
    }

    @Test
    @DisplayName("init() com bucket vazio lança IllegalArgumentException")
    void init_emptyBucket_throws() {
        assertThrows(IllegalArgumentException.class, () ->
            ODMMetricsManager.init(null, "  ", "prefix", "us-east-1"));
    }

    @Test
    @DisplayName("init() com SparkContext null lança IllegalArgumentException")
    void init_nullSc_throws() {
        assertThrows(IllegalArgumentException.class, () ->
            ODMMetricsManager.init(null, "meu-bucket", "prefix", "us-east-1"));
    }

    // -------------------------------------------------------------------------
    // flush() sem init() — lança IllegalStateException
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("flush() sem init() lança IllegalStateException")
    void flush_withoutInit_throwsIllegalStateException() {
        assertThrows(IllegalStateException.class, () ->
            ODMMetricsManager.flush(
                100L, 99L, 1L, 5000L, "/my/ruleset/1.0/test",
                System.currentTimeMillis() - 5000, System.currentTimeMillis()
            )
        );
    }

    // -------------------------------------------------------------------------
    // flush() com totalCount=0 — retorna sem tentar S3
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("flush() com totalCount=0 não lança exceção e não tenta S3")
    void flush_zeroCount_doesNotThrow() throws Exception {
        // Simular init() sem SparkContext: setar s3Bucket diretamente
        setField("s3Bucket", "meu-bucket");
        assertDoesNotThrow(() ->
            ODMMetricsManager.flush(
                0L, 0L, 0L, 0L, "/my/ruleset/1.0/test",
                System.currentTimeMillis(), System.currentTimeMillis()
            )
        );
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static void setField(String name, Object value) throws Exception {
        Field f = ODMMetricsManager.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(null, value);
    }
}
