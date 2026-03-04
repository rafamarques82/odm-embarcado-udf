# ODM Embarcado - Generic UDF

UDF genérica para execução de regras ODM no Apache Spark, com suporte a Kafka e S3.

## 📁 Estrutura do Projeto

```
Embarcado/
├── pom.xml                          # Maven build configuration
├── lib/                             # ODM libraries (não versionadas)
│   ├── jrules-engine.jar
│   ├── jrules-res-execution.jar
│   └── license_metric_logger_*.jar
└── src/main/java/br/com/itau/odm/embarcado/
    ├── GenericODMUDF.java          # UDF principal
    ├── FacadeSessionFactory.java    # Factory para sessões ODM
    ├── FacadeStatelessSession.java  # Wrapper de sessão
    ├── DecisionMetering.java        # Metering de decisões
    ├── DecisionMeteringReport.java  # Relatórios de métricas
    ├── KafkaMetrics.java            # Envio de métricas para Kafka
    └── S3Metrics.java               # Envio de métricas para S3
```

## 🚀 Funcionalidades

### GenericODMUDF
- ✅ Execução de regras ODM via reflection (qualquer XOM)
- ✅ Integração com Kafka (métricas em tempo real)
- ✅ Integração com S3 (relatórios ILMT)
- ✅ Thread-safe e otimizada para Spark
- ✅ Modo XU MEMORY para alta performance

### KafkaMetrics
- ✅ Envio assíncrono de métricas para Kafka
- ✅ Buffer configurável (tamanho e tempo)
- ✅ Compatível com IBM Event Streams
- ✅ Shutdown hook para flush automático

### S3Metrics
- ✅ Geração de relatórios ILMT (XML)
- ✅ Particionamento automático por data/hora
- ✅ Acumuladores thread-safe
- ✅ Compatível com IBM License Metric Tool

## 🔧 Compilação

### Pré-requisitos
- Java 11+
- Maven 3.6+
- JARs ODM 8.12.0 (colocar em `lib/`)

### Build
```bash
cd Embarcado
mvn clean package
```

O JAR será gerado em: `target/odm-embarcado-udf-1.0.jar`

## 📦 Dependências

### ODM Libraries (não incluídas)
Você precisa adicionar os JARs do ODM 8.12.0 em `lib/`:
- `jrules-engine.jar`
- `jrules-res-execution.jar`
- `license_metric_logger_2.1.2.202009181115.jar`

### Maven Dependencies
- Apache Spark 3.3.0
- Kafka Clients 3.3.1
- AWS SDK S3 1.12.x
- Jackson 2.13.x

## ⚙️ Configuração

### Kafka (opcional)
Crie `kafka-config.properties`:
```properties
bootstrap.servers=broker:9093
topic=odm_kafka_events
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=...
```

### S3 (opcional)
Crie `s3-config.properties`:
```properties
s3.metrics.bucket=meu-bucket
s3.metrics.prefix=odm-metrics/
s3.metrics.region=us-east-1
```

## 🎯 Uso no Spark

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Registrar UDF
spark.sparkContext.addJar("s3://bucket/odm-embarcado-udf-1.0.jar")
spark.sparkContext.addJar("s3://bucket/seu-ruleset.jar")

# Configuração
config = {
    "ruleset_path": "/tmp/seu-ruleset.jar",
    "xom_path": "/tmp/seu-xom.jar",
    "input_class": "com.example.Input",
    "output_class": "com.example.Output"
}

# Criar UDF
odm_udf = udf(
    lambda json_input: spark.sparkContext._jvm.br.com.itau.odm.embarcado.GenericODMUDF.executeDecision(
        json_input,
        spark.sparkContext._jvm.scala.collection.JavaConverters.mapAsScalaMap(config)
    ),
    StringType()
)

# Usar
df_result = df.withColumn("resultado", odm_udf("cenario"))
```

## 📊 Métricas

### Kafka
Envia em tempo real:
- Timestamp de execução
- Duração (ms)
- Regras disparadas
- Status (sucesso/erro)
- Ruleset path

### S3 (ILMT)
Envia no shutdown:
- Relatório ILMT (XML)
- Relatório customizado (XML)
- Estatísticas agregadas
- Particionamento: `yyyy/MM/dd/HH/`

## 🔒 Segurança

- ✅ Credenciais via AWS Secrets Manager (recomendado)
- ✅ IAM Roles para Glue/EMR
- ✅ Sem senhas hardcoded no código
- ✅ SSL/TLS para Kafka

## 📝 Licença

Propriedade do Itaú Unibanco

## 👥 Autores

Equipe BRE - Itaú Unibanco