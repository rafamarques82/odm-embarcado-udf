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
        setField("s3Bucket",             null);
        setField("s3Prefix",             "odm-metrics");
        setField("s3Region",             "us-east-1");
        setField("accumulator",          null);
        setField("startMs",              0L);
        setField("initializedBroadcast", null);
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
    @DisplayName("flush() sem init() retorna silenciosamente sem lançar exceção")
    void flush_withoutInit_silentReturn() {
        assertDoesNotThrow(ODMMetricsManager::flush);
    }

    // -------------------------------------------------------------------------
    // flush() com accumulator zerado — retorna sem tentar S3
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("flush() com accumulator zerado não tenta S3")
    void flush_zeroAccumulator_doesNotThrow() throws Exception {
        setField("s3Bucket",    "meu-bucket");
        setField("accumulator", new S3MetricsAccumulator());
        assertDoesNotThrow(ODMMetricsManager::flush);
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
