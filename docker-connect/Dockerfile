FROM confluentinc/cp-kafka-connect:7.4.11

USER root
COPY install-connectors.sh /tmp/install-connectors.sh
RUN chmod +x /tmp/install-connectors.sh && /tmp/install-connectors.sh

USER appuser