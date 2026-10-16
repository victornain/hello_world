# Build context: activemq-sqs-bridge/ (see docker-compose.yml)
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY . .
RUN mvn -B -q -pl sqs-to-amq -am package -DskipTests

FROM eclipse-temurin:17-jre
RUN useradd --system --uid 10001 bridge && mkdir -p /app
COPY --from=build /build/sqs-to-amq/target/sqs-to-amq.jar /app/app.jar
USER bridge
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
