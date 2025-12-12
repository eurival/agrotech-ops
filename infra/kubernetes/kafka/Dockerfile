# Usa a imagem base oficial do Strimzi (mesma versão do seu cluster)
FROM quay.io/strimzi/kafka:0.40.0-kafka-3.7.0

# Muda para root para poder instalar coisas
USER root:root

# 1. Cria a pasta de plugins
RUN mkdir -p /opt/kafka/plugins/mqtt

# 2. Baixa o plugin da Lenses.io (Versão 8.0)
# Usamos curl (que vem na imagem) e tar/unzip
RUN curl -L -o /tmp/plugin.zip https://github.com/lensesio/stream-reactor/releases/download/8.0.0/kafka-connect-mqtt-8.0.0.zip && \
    unzip -q /tmp/plugin.zip -d /tmp/ && \
    mv /tmp/kafka-connect-mqtt-8.0.0/*.jar /opt/kafka/plugins/mqtt/ && \
    rm -f /tmp/plugin.zip

# 3. O PULO DO GATO: Ajustar permissões
# O Strimzi roda como usuário 1001. Se a pasta for do root, ele não consegue ler/executar.
RUN chown -R 1001:0 /opt/kafka/plugins

# Volta para o usuário padrão do Strimzi (Segurança)
USER 1001