# Usa a imagem base oficial do Strimzi
FROM quay.io/strimzi/kafka:0.40.0-kafka-3.7.0

USER root:root

# 1. Instala utilitários
RUN microdnf install -y unzip wget curl

# 2. Cria a pasta de plugins e baixa o conector Lenses
RUN mkdir -p /opt/kafka/plugins/mqtt && \
    curl -L -o /tmp/plugin.zip https://github.com/lensesio/stream-reactor/releases/download/8.0.0/kafka-connect-mqtt-8.0.0.zip && \
    unzip -q /tmp/plugin.zip -d /tmp/ && \
    mv /tmp/kafka-connect-mqtt-8.0.0/*.jar /opt/kafka/plugins/mqtt/ && \
    rm -f /tmp/plugin.zip

# 3. O PULO DO GATO FINAL: CRIA O ARQUIVO DE LOG VAZIO
# Isso resolve o erro de FileNotFoundException
RUN mkdir -p /opt/kafka/custom-config && \
    touch /opt/kafka/custom-config/log4j.properties

# 4. Ajustar permissões para o usuário 1001 (Strimzi)
RUN chown -R 1001:0 /opt/kafka/plugins /opt/kafka/custom-config

# Volta para o usuário padrão do Strimzi
USER 1001