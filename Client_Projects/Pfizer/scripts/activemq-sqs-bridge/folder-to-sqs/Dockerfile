# Build context: activemq-sqs-bridge/ (see docker-compose.yml)
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY . .
RUN mvn -B -q -pl folder-to-sqs -am package -DskipTests

FROM eclipse-temurin:17-jre
RUN useradd --system --uid 10001 bridge && mkdir -p /app /data/messages /data/failed && chown bridge /data/messages /data/failed
COPY --from=build /build/folder-to-sqs/target/folder-to-sqs.jar /app/app.jar
USER bridge
VOLUME ["/data/messages", "/data/failed"]
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar \"$@\"", "--"]
