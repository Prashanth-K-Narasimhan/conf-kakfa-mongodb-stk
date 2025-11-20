FROM confluentinc/cp-kafka-connect:7.4.11

USER root

RUN if command -v microdnf >/dev/null 2>&1; then \
      microdnf install -y python39 python39-pip jq nmap-ncat && microdnf clean all; \
    elif command -v dnf >/dev/null 2>&1; then \
      dnf install -y python39 python39-pip jq nmap-ncat && dnf clean all; \
    else \
      echo "No supported package manager found" >&2; exit 1; \
    fi && \
    python3.9 -m pip install --no-cache-dir pymongo==4.6.0

RUN mkdir -p /usr/share/confluent-hub-components && chown -R 1001:0 /usr/share/confluent-hub-components

USER 1001
