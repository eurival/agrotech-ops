# Usa a imagem base oficial do Strimzi
FROM quay.io/strimzi/kafka:0.40.0-kafka-3.7.0

USER root:root

# 1. Baixa o plugin da Lenses.io (Versão 8.0)
# Usamos curl para baixar e unzip para extrair
RUN curl -L -o /tmp/plugin.zip https://github.com/lensesio/stream-reactor/releases/download/8.1.33/kafka-connect-mqtt-8.1.33.zip && \
    mkdir -p /opt/kafka/plugins/mqtt && \
    unzip -q /tmp/plugin.zip -d /tmp/ && \
    mv /tmp/kafka-connect-mqtt-8.1.33/*.jar /opt/kafka/plugins/mqtt/ && \
    rm -f /tmp/plugin.zip

# 2. O PULO DO GATO 😺: Corrigir as permissões
# O Strimzi roda como usuário 1001. Passamos a posse da pasta para ele.
RUN chown -R 1001:0 /opt/kafka/plugins

# Volta para o usuário padrão do Strimzi
USER 1001