# USANDO A TAG EXISTENTE QUE SUPORTA O KAFKA 4.1.1
FROM quay.io/strimzi/kafka:latest-kafka-4.1.1

USER root:root

# REMOVEMOS: RUN microdnf install -y unzip wget curl
# Vamos apenas tentar instalar unzip e wget (que não devem conflitar)
RUN microdnf install -y unzip wget

# 1. Cria a pasta de plugins e baixa o conector Lenses
RUN mkdir -p /opt/kafka/plugins/mqtt && \
    curl -L -o /tmp/plugin.zip https://github.com/lensesio/stream-reactor/releases/download/11.1.0/kafka-connect-mqtt-11.1.0.zip && \
    unzip -q /tmp/plugin.zip -d /tmp/ && \
    mv /tmp/kafka-connect-mqtt-11.1.0/*.jar /opt/kafka/plugins/mqtt/ && \
    rm -f /tmp/plugin.zip

# 2. HACK FINAL: Criamos o arquivo log4j.properties que o Java procura
RUN mkdir -p /opt/kafka/custom-config && \
    touch /opt/kafka/custom-config/log4j.properties

# 3. Ajustar permissões
RUN chown -R 1001:0 /opt/kafka/plugins /opt/kafka/custom-config

# Volta para o usuário padrão do Strimzi
USER 1001