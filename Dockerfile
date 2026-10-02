# One Dockerfile for every service: the build stage is shared (and cached)
# across images, the runtime stage picks a jar with --build-arg JAR=...
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
COPY proto proto
COPY mock-psp mock-psp
COPY eligibility-service eligibility-service
COPY payment-service payment-service
COPY loadtest loadtest
RUN mvn -q -B package -DskipTests

FROM eclipse-temurin:21-jre
ARG JAR
COPY --from=build /src/${JAR} /app/app.jar
USER 1000
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
