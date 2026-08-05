FROM confluentinc/cp-kafka:7.7.0

USER root

# Create directory for the exporter
RUN mkdir -p /usr/share/jmx_exporter

# Download the Prometheus JMX Java agent
RUN curl -L -o /usr/share/jmx_exporter/jmx_prometheus_javaagent-0.19.0.jar \
    https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.19.0/jmx_prometheus_javaagent-0.19.0.jar

# Copy the configuration file into the image
COPY kafka-jmx.yml /usr/share/jmx_exporter/kafka-jmx.yml

# Secure permissions
RUN chown -R appuser:appuser /usr/share/jmx_exporter

USER appuser