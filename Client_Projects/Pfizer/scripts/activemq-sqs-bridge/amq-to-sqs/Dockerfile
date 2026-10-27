# Build context: activemq-sqs-bridge/ (see docker-compose.yml)
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY . .
RUN mvn -B -q -pl amq-to-sqs -am package -DskipTests

FROM eclipse-temurin:17-jre AS java-runtime

FROM python:3.13-slim-bookworm
COPY --from=java-runtime /opt/java/openjdk /opt/java/openjdk
RUN python -m venv /opt/boto3-venv \
	&& mkdir -p /app /home/bridge \
	&& chown -R 10001:10001 /app /home/bridge
COPY amq-to-sqs/requirements.txt /app/requirements.txt
RUN /opt/boto3-venv/bin/pip install --no-cache-dir -r /app/requirements.txt
COPY --from=build /build/amq-to-sqs/target/amq-to-sqs.jar /app/app.jar
COPY amq-to-sqs/src/main/python/sqs_publisher.py /app/sqs_publisher.py
USER 10001:10001
ENV JAVA_HOME="/opt/java/openjdk"
ENV HOME="/home/bridge"
ENV PATH="/opt/boto3-venv/bin:/opt/java/openjdk/bin:${PATH}"
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75"
ENV PYTHON_EXECUTABLE="python3"
ENV PYTHON_PUBLISHER_SCRIPT="/app/sqs_publisher.py"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar \"$@\"", "--"]
