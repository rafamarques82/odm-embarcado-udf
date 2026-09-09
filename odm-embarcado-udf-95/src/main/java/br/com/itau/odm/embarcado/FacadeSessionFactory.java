package br.com.itau.odm.embarcado;

// ==== IMPORTS ODM 8.12 ====
import ilog.rules.res.session.IlrJ2SESessionFactory;
import ilog.rules.res.session.IlrStatelessSession;
import ilog.rules.res.session.IlrSessionCreationException;
import ilog.rules.res.session.config.IlrSessionFactoryConfig;

/**
 * Factory alinhada ao pacote do FacadeStatelessSession + KafkaMetrics (ODM 8.12).
 * - Declara IlrSessionCreationException conforme a sua instalação.
 * - Removidos imports de teamserver (não necessários para execução embarcada)
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
        // Kafka é opcional - não validar aqui para permitir testes sem Kafka
        IlrStatelessSession delegate = super.createStatelessSession();
        return new FacadeStatelessSession(delegate); // agora IMPLEMENTA IlrStatelessSession
    }

    /** Opcional: forçar envio do resumo ao final do job. */
    public void close() {
        KafkaMetrics.close();
    }
}

// Made with Bob
