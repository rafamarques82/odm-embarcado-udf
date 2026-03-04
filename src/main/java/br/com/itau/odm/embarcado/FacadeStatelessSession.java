
package br.com.itau.odm.embarcado;

// ==== IMPORTS ODM 8.12 ====
import ilog.rules.res.session.IlrSessionException;
import ilog.rules.res.session.IlrSessionRequest;
import ilog.rules.res.session.IlrSessionResponse;
import ilog.rules.res.session.IlrStatelessSession;

// Observer e eventos assíncronos (API oficial)
import ilog.rules.res.session.async.IlrAsyncExecutionObserver;
import ilog.rules.res.session.async.IlrAsyncExecutionEvent;
import ilog.rules.res.session.async.IlrAsyncExecutionEndedEvent;
import ilog.rules.res.session.async.IlrAsyncExecutionFailedEvent;

// ==== IMPORTS JDK ====
import java.lang.reflect.Method;

/**
 * Facade sobre IlrStatelessSession (ODM 8.12).
 * - IMPLEMENTA IlrStatelessSession (síncrono + assíncrono).
 * - Captura o ruleset do request e atualiza KafkaMetrics.
 * - Fail-fast de Kafka antes da execução.
 * - Mede duração e registra métricas (ok/erro, duração, regras disparadas).
 */
public class FacadeStatelessSession implements IlrStatelessSession {

    private final IlrStatelessSession delegate;

    public FacadeStatelessSession(IlrStatelessSession delegate) {
        if (delegate == null) {
            throw new IllegalArgumentException("Delegate IlrStatelessSession não pode ser null");
        }
        this.delegate = delegate;
    }

    // ===================== SÍNCRONO =====================
    @Override
    public IlrSessionResponse execute(IlrSessionRequest req) throws IlrSessionException {
        final long t0 = System.nanoTime();

        // 1) Captura RULESET real do request
        updateRulesetIfPresent(req);

        // 2) Fail-fast de Kafka
        KafkaMetrics.validateOrThrow();

        try {
            // 3) Executa o delegate
            IlrSessionResponse resp = delegate.execute(req);
            long durMs = (System.nanoTime() - t0) / 1_000_000L;

            // 4) Extrai "totalRulesFired" via reflection
            Integer rulesFired = tryGetTotalRulesFired(resp);

            // 5) Registra sucesso
            KafkaMetrics.recordExecution("ok", durMs, rulesFired);
            return resp;

        } catch (IlrSessionException e) {
            long durMs = (System.nanoTime() - t0) / 1_000_000L;
            KafkaMetrics.recordExecution("error", durMs, null);
            throw e;
        } catch (RuntimeException e) {
            long durMs = (System.nanoTime() - t0) / 1_000_000L;
            KafkaMetrics.recordExecution("error", durMs, null);
            throw e;
        }
    }

    // ===================== ASSÍNCRONO =====================
    @Override
    public void executeAsynchronous(
            IlrSessionRequest req,
            IlrAsyncExecutionObserver observer,
            long timeout) throws IlrSessionException {

        final long t0 = System.nanoTime();

        // 1) Captura RULESET real do request
        updateRulesetIfPresent(req);

        // 2) Fail-fast de Kafka
        KafkaMetrics.validateOrThrow();

        // 3) Encapsula o observer para registrar métricas ao final
        IlrAsyncExecutionObserver wrapped = new IlrAsyncExecutionObserver() {
            @Override
            public void update(IlrAsyncExecutionEvent event) {
                long durMs = (System.nanoTime() - t0) / 1_000_000L;
                try {
                    if (event instanceof IlrAsyncExecutionEndedEvent) {
                        IlrAsyncExecutionEndedEvent ended = (IlrAsyncExecutionEndedEvent) event;
                        IlrSessionResponse resp = ended.getResponse();
                        Integer rulesFired = tryGetTotalRulesFired(resp);
                        KafkaMetrics.recordExecution("ok", durMs, rulesFired);
                    } else if (event instanceof IlrAsyncExecutionFailedEvent) {
                        IlrAsyncExecutionFailedEvent failed = (IlrAsyncExecutionFailedEvent) event;
                        // podemos logar failed.getException() se quiser
                        KafkaMetrics.recordExecution("error", durMs, null);
                    }
                } finally {
                    // Propaga para o observer do chamador (se fornecido)
                    if (observer != null) {
                        observer.update(event);
                    }
                }
            }
        };

        // 4) Dispara no delegate (não bloqueia)
        delegate.executeAsynchronous(req, wrapped, timeout);
    }

    // ===================== HELPERS =====================

    private static void updateRulesetIfPresent(IlrSessionRequest req) {
        try {
            if (req != null && req.getRulesetPath() != null) {
                String rulePath = String.valueOf(req.getRulesetPath());
                if (rulePath != null && !rulePath.isBlank()) {
                    KafkaMetrics.updateRuleset(rulePath); // mesmo pacote => acesso OK
                }
            }
        } catch (Exception ignore) { /* diagnóstico não deve derrubar execução */ }
    }

    private static Integer tryGetTotalRulesFired(IlrSessionResponse resp) {
        if (resp == null) return null;
        try {
            Object trace = resp.getRulesetExecutionTrace();
            if (trace == null) return null;

            Method m = trace.getClass().getMethod("getTotalRulesFired");
            Object val = m.invoke(trace);
            if (val instanceof Number) {
                long lf = ((Number) val).longValue();
                return (lf <= Integer.MAX_VALUE) ? (int) lf : Integer.MAX_VALUE;
            }
        } catch (Exception ignore) { }
        return null;
    }

    /** Utilitário opcional caso queira fechar delegate por reflection. */
    public void closeQuietly() { /* no-op */ }
}
