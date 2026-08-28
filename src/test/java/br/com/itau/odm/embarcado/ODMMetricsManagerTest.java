package br.com.itau.odm.embarcado;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Testes unitários para ODMMetricsManager.
 *
 * init(sc, bucket, prefix, region) valida, registra Accumulator, propaga para executores
 * e registra SparkListener para flush() automático.
 * flush() é chamado automaticamente — mas também pode ser chamado manualmente.
 */
@DisplayName("ODMMetricsManager")
class ODMMetricsManagerTest {

    @AfterEach
    void resetState() throws Exception {
        setField("s3Bucket",    null);
        setField("s3Prefix",    "odm-metrics");
        setField("s3Region",    "us-east-1");
        setField("accumulator", null);
        setField("startMs",     0L);
        setField("flushed",     false);
    }

    // -------------------------------------------------------------------------
    // init() — validação de parâmetros (sem SparkContext real)
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("init() com sc null lança IllegalArgumentException")
    void init_nullSc_throws() {
        assertThrows(IllegalArgumentException.class, () ->
            ODMMetricsManager.init(null, "meu-bucket", "prefix", "us-east-1"));
    }

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

    // -------------------------------------------------------------------------
    // flush() sem init() — retorna silenciosamente (listener pode chamar antes do init)
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("flush() sem init() lança IllegalStateException")
    void flush_withoutInit_throws() {
        assertThrows(IllegalStateException.class, () ->
            ODMMetricsManager.flush(100L, 99L, 1L, 5000L, "/r/1.0/t",
                System.currentTimeMillis() - 1000, System.currentTimeMillis()));
    }

    // -------------------------------------------------------------------------
    // flush() com totalCount=0 — retorna sem tentar S3
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("flush() com totalCount=0 não tenta S3")
    void flush_zeroCount_doesNotThrow() throws Exception {
        setField("s3Bucket", "meu-bucket");
        assertDoesNotThrow(() ->
            ODMMetricsManager.flush(0L, 0L, 0L, 0L, "/r/1.0/t",
                System.currentTimeMillis(), System.currentTimeMillis()));
    }

    // -------------------------------------------------------------------------
    // flushRequired()
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("flushRequired() após init() sem flush() lança IllegalStateException")
    void flushRequired_withoutFlush_throws() throws Exception {
        setField("s3Bucket", "meu-bucket");
        setField("flushed",  false);
        assertThrows(IllegalStateException.class, ODMMetricsManager::flushRequired);
    }

    @Test
    @DisplayName("flushRequired() após flush() não lança exceção")
    void flushRequired_afterFlush_doesNotThrow() throws Exception {
        setField("s3Bucket", "meu-bucket");
        setField("flushed",  true);
        assertDoesNotThrow(ODMMetricsManager::flushRequired);
    }

    @Test
    @DisplayName("flushRequired() sem init() não lança exceção")
    void flushRequired_withoutInit_doesNotThrow() {
        // s3Bucket é null — init() não foi chamado — flushRequired() é no-op
        assertDoesNotThrow(ODMMetricsManager::flushRequired);
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
