package br.com.itau.odm.embarcado;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Testes unitários para ODMMetricsManager.
 *
 * Como o manager não tem mais estado (sem init/flush com Spark), testamos:
 *   - flushWithData() com bucket null lança IllegalArgumentException
 *   - flushWithData() com totalCount == 0 retorna sem chamar S3
 *   - flushWithData() com bucket vazio lança IllegalArgumentException
 */
@DisplayName("ODMMetricsManager")
class ODMMetricsManagerTest {

    // -------------------------------------------------------------------------
    // Validação de bucket obrigatório
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("flushWithData() com bucket null lança IllegalArgumentException")
    void flushWithData_nullBucket_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () ->
            ODMMetricsManager.flushWithData(
                null, "prefix", "us-east-1",
                100L, 99L, 1L, 5000L, 0L, "/my/ruleset/1.0/test",
                System.currentTimeMillis() - 5000, System.currentTimeMillis()
            )
        );
    }

    @Test
    @DisplayName("flushWithData() com bucket vazio lança IllegalArgumentException")
    void flushWithData_emptyBucket_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () ->
            ODMMetricsManager.flushWithData(
                "  ", "prefix", "us-east-1",
                100L, 99L, 1L, 5000L, 0L, "/my/ruleset/1.0/test",
                System.currentTimeMillis() - 5000, System.currentTimeMillis()
            )
        );
    }

    // -------------------------------------------------------------------------
    // totalCount == 0 — deve retornar sem tentar enviar para S3
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("flushWithData() com totalCount 0 não lança exceção e não tenta enviar S3")
    void flushWithData_zeroCount_doesNotThrow() {
        // Se tentasse enviar para S3, lançaria exception de conexão
        assertDoesNotThrow(() ->
            ODMMetricsManager.flushWithData(
                "meu-bucket", "odm-metrics", "sa-east-1",
                0L, 0L, 0L, 0L, 0L, "/my/ruleset/1.0/test",
                System.currentTimeMillis(), System.currentTimeMillis()
            )
        );
    }

    // -------------------------------------------------------------------------
    // Parâmetros com defaults (prefix/region nulos usam default)
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("flushWithData() com prefix e region nulos usa defaults e não lança exceção (totalCount=0)")
    void flushWithData_nullPrefixAndRegion_usesDefaults() {
        assertDoesNotThrow(() ->
            ODMMetricsManager.flushWithData(
                "meu-bucket", null, null,
                0L, 0L, 0L, 0L, 0L, "/my/ruleset/1.0/test",
                System.currentTimeMillis(), System.currentTimeMillis()
            )
        );
    }
}
