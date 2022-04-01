FROM eclipse-temurin:17-jdk-alpine AS builder
COPY . /app
RUN ls /app
WORKDIR /app
RUN ./gradlew clean build -x test
RUN chmod 777 /app/storage/build/libs/storage-0.0.0-SNAPSHOT.jar

FROM eclipse-temurin:17-jdk-alpine
COPY --from=builder /app/private/dev/gifterKeyStore.ts /app/gifterKeyStore.ts
ENV GIFTER_SSL_TRUST_STORE=/app/gifterKeyStore.ts
COPY --from=builder /app/storage/build/libs/storage-0.0.0-SNAPSHOT.jar /app/app.jar
CMD ["java",  "-jar", "/app/app.jar"]
