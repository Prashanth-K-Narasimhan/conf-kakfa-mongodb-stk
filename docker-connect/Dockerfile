# docker-connect/Dockerfile
# Not required when we mount plugin jars from host. for reference.
FROM confluentinc/cp-kafka-connect:7.4.11

USER root
COPY install-plugins.sh /tmp/install-plugins.sh
RUN chmod +x /tmp/install-plugins.sh && /tmp/install-plugins.sh

USER 1001
