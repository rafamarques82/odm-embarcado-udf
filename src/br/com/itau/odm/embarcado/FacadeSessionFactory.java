
package br.com.itau.odm.embarcado;

// ==== IMPORTS ODM 8.12 ====
import ilog.rules.res.session.IlrJ2SESessionFactory;
import ilog.rules.res.session.IlrStatelessSession;
import ilog.rules.res.session.IlrSessionCreationException;        // <- tratar na assinatura
import ilog.rules.res.session.config.IlrSessionFactoryConfig;     // <- pacote correto no 8.12
import ilog.rules.teamserver.brm.*;
import ilog.rules.teamserver.client.IlrRemoteSessionFactory;
import ilog.rules.teamserver.model.*;

/**
 * Factory alinhada ao pacote do FacadeStatelessSession + KafkaMetrics (ODM 8.12).
 * - Declara IlrSessionCreationException conforme a sua instalação.
 */
public class FacadeSessionFactory extends IlrJ2SESessionFactory {

    public static IlrSessionFactoryConfig createDefaultConfig() {
        return IlrJ2SESessionFactory.createDefaultConfig();
    }

    public FacadeSessionFactory(IlrSessionFactoryConfig config) {
        super(config);
    }

    @Override
    public IlrStatelessSession createStatelessSession() throws IlrSessionCreationException {
        // Fail-fast leve antes de criar a sessão
        KafkaMetrics.validateOrThrow();

        IlrStatelessSession delegate = super.createStatelessSession();
        return new FacadeStatelessSession(delegate); // agora IMPLEMENTA IlrStatelessSession
    }

    /** Opcional: forçar envio do resumo ao final do job. */
    public void close() {
        KafkaMetrics.close();
    }
}
