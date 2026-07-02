FROM apache/hive:4.0.0

USER root

# apache/hive:4.0.0 is a minimal image — curl/wget not included.
# Install curl, download the PostgreSQL JDBC driver, then remove curl
# to keep the image lean.
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    curl -fSL "https://jdbc.postgresql.org/download/postgresql-42.7.3.jar" \
         -o /opt/hive/lib/postgresql-42.7.3.jar && \
    apt-get purge -y curl && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

USER hive
