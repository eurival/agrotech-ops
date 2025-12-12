# Usamos a mesma imagem base que estava no YAML
FROM quay.io/strimzi/kafka:0.40.0-kafka-3.7.0

USER root:root

# Cria a pasta de plugins
RUN mkdir -p /opt/kafka/plugins/mqtt

# Baixa o conector Lenses (Versão 8.0 estável) e instala
# Usando curl e unzip que geralmente já tem na base, ou instalamos se faltar
RUN microdnf install -y unzip wget && \
    wget -q -O /tmp/plugin.zip https://github.com/lensesio/stream-reactor/releases/download/8.0.0/kafka-connect-mqtt-8.0.0.zip && \
    unzip -q /tmp/plugin.zip -d /tmp/ && \
    mv /tmp/kafka-connect-mqtt-8.0.0/*.jar /opt/kafka/plugins/mqtt/ && \
    rm -f /tmp/plugin.zip && \
    ls -l /opt/kafka/plugins/mqtt

# Volta para o usuário padrão do Strimzi
USER 1001