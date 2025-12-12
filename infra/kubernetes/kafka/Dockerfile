# Usa a imagem base oficial do Strimzi
FROM quay.io/strimzi/kafka:latest-kafka-4.1.1

USER root:root

# Instala utilitários
RUN microdnf install -y unzip wget curl

# 1. Cria a pasta de plugins e baixa o conector Lenses
RUN curl -L -o /tmp/plugin.zip https://github.com/lensesio/stream-reactor/releases/download/8.0.0/kafka-connect-mqtt-8.0.0.zip && \
    mkdir -p /opt/kafka/plugins/mqtt && \
    unzip -q /tmp/plugin.zip -d /tmp/ && \
    mv /tmp/kafka-connect-mqtt-8.0.0/*.jar /opt/kafka/plugins/mqtt/ && \
    rm -f /tmp/plugin.zip

# 2. HACK FINAL: Criamos a pasta custom-config e garantimos que o log4j.properties exista.
# Como o Strimzi sempre cria o log4j2.properties, vamos criar o VAZIO que o Java procura.
RUN mkdir -p /opt/kafka/custom-config && \
    touch /opt/kafka/custom-config/log4j.properties

# 3. Ajustar permissões
RUN chown -R 1001:0 /opt/kafka/plugins /opt/kafka/custom-config

# Volta para o usuário padrão do Strimzi
USER 1001