FROM apache/hive:4.0.0

USER root

# Installer wget, netcat et télécharger le driver PostgreSQL JDBC
RUN apt-get update && apt-get install -y wget netcat && \
    wget https://jdbc.postgresql.org/download/postgresql-42.7.1.jar -O /opt/hive/lib/postgresql-jdbc.jar && \
    chmod 644 /opt/hive/lib/postgresql-jdbc.jar && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER hive

EXPOSE 9083
