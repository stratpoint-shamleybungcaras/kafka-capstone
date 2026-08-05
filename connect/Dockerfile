FROM confluentinc/cp-kafka-connect:7.7.0

# Install the sink connector for JDBC
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.9.4 

# Install the MongoDB Sink Plugin
RUN confluent-hub install --no-prompt mongodb/kafka-connect-mongodb:1.11.2

# Install the PostgreSQL JDBC driver jar
USER root
RUN curl -o /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/postgresql-42.7.11.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.11.jar

USER appuser