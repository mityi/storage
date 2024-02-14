FROM eclipse-temurin:21-jdk-alpine AS builder
COPY . /app
RUN ls /app
WORKDIR /app
RUN ./gradlew clean build -x test
RUN chmod 777 /app/storage/build/libs/storage-0.0.0-SNAPSHOT.jar

FROM eclipse-temurin:21-jdk-alpine
COPY --from=builder /app/storage/build/libs/storage-0.0.0-SNAPSHOT.jar /app/app.jar
CMD ["java",  "-jar", "/app/app.jar"]
