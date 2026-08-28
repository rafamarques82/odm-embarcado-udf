package br.com.itau.odm.embarcado;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Testes unitários para ODMMetricsManager.
 */
@DisplayName("ODMMetricsManager")
class ODMMetricsManagerTest {

    // -------------------------------------------------------------------------
    // Validação: bucket obrigatório
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("flush() com bucket null lança IllegalArgumentException")
    void flush_nullBucket_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () ->
            ODMMetricsManager.flush(
                null, "prefix", "us-east-1",
                100L, 99L, 1L, 5000L, "/my/ruleset/1.0/test",
                System.currentTimeMillis() - 5000, System.currentTimeMillis()
            )
        );
    }

    @Test
    @DisplayName("flush() com bucket vazio lança IllegalArgumentException")
    void flush_emptyBucket_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () ->
            ODMMetricsManager.flush(
                "  ", "prefix", "us-east-1",
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
    void flush_zeroCount_doesNotThrow() {
        // totalCount=0 → retorna antes de qualquer I/O de rede
        assertDoesNotThrow(() ->
            ODMMetricsManager.flush(
                "meu-bucket", "odm-metrics", "sa-east-1",
                0L, 0L, 0L, 0L, "/my/ruleset/1.0/test",
                System.currentTimeMillis(), System.currentTimeMillis()
            )
        );
    }

    // -------------------------------------------------------------------------
    // prefix e region nulos usam defaults
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("flush() com prefix/region nulos usa defaults e não lança exceção (totalCount=0)")
    void flush_nullPrefixAndRegion_usesDefaults() {
        assertDoesNotThrow(() ->
            ODMMetricsManager.flush(
                "meu-bucket", null, null,
                0L, 0L, 0L, 0L, "/my/ruleset/1.0/test",
                System.currentTimeMillis(), System.currentTimeMillis()
            )
        );
    }
}
