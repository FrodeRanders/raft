FROM eclipse-temurin:21-jre

WORKDIR /app
COPY raft-dist/target/raft-1.0-SNAPSHOT.jar /app/raft.jar

EXPOSE 7000
ENTRYPOINT ["java", "-jar", "/app/raft.jar"]
