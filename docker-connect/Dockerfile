# docker-connect/Dockerfile
# Build a kafka-connect image that will attempt to install connectors,
# fall back to local jars, create required topics and apply ksql files.
FROM confluentinc/cp-kafka-connect:7.4.11

USER root

# copy connector jar(s) from host (if you pre-downloaded them)
# they will be used if the "confluent-hub" install fails
COPY connect-plugins /tmp/connect-plugins-local

# copy helper scripts
COPY install-plugins.sh /tmp/install-plugins.sh
COPY /scripts/create-topics.sh /tmp/create-topics.sh
COPY /scripts/run_ksql_files.sh /tmp/run_ksql_files.sh
COPY connectors /tmp/connectors
COPY ksql /tmp/ksql

# entrypoint wrapper (will be copied below)
COPY entrypoint-init.sh /usr/local/bin/entrypoint-init.sh

RUN chmod +x /tmp/install-plugins.sh /tmp/create-topics.sh /tmp/run_ksql_files.sh /usr/local/bin/entrypoint-init.sh \
  && sync

RUN apt-get update && apt-get install -y jq netcat && rm -rf /var/lib/apt/lists/*

# Make sure the default user is used after installation steps
USER 1001

# Use our init entrypoint that will perform init tasks then exec the original connect start
ENTRYPOINT ["/usr/local/bin/entrypoint-init.sh"]
# Keep the same default CMD from base image (this will be executed at the end of our entrypoint)
CMD ["/etc/confluent/docker/run"]
