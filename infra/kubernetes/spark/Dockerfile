# Usa a versão 3.5.3 do repositório legacy
FROM bitnamilegacy/spark:3.5.3

USER root

# --- CORREÇÃO AQUI: Instala o CURL primeiro ---
RUN apt-get update && apt-get install -y curl

# Agora sim, baixa os drivers (Postgres e Kafka)
RUN curl -o /opt/bitnami/spark/jars/postgresql-42.6.0.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
RUN curl -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar
RUN curl -o /opt/bitnami/spark/jars/kafka-clients-3.5.0.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar

# Dependências extras
RUN curl -o /opt/bitnami/spark/jars/commons-pool2-2.11.1.jar https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar
RUN curl -o /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar

# Copia seu script
COPY processor.py /app/processor.py

WORKDIR /app