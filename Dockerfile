FROM confluentinc/cp-kafka-connect:7.4.1

COPY ./pipeline/connect-plugins/mongo-kafka-connect-1.10.0-all.jar /usr/share/java/
COPY ./pipeline/connect-plugins/transforms-for-apache-kafka-connect-1.6.0.jar /usr/share/java/
COPY ./pipeline/connect-plugins/slf4j-api-1.7.36.jar /usr/share/java/

USER 1001
