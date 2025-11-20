FROM confluentinc/cp-kafka-connect:7.4.1

# Copie o seu JAR personalizado para o diretório de plugins do Kafka Connect
COPY ./connect-plugins/mongo-kafka-connect-1.10.0-all.jar /usr/share/java/

USER 1001
