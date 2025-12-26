FROM eclipse-temurin:17-jdk

WORKDIR /app

RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

RUN wget https://jdbc.postgresql.org/download/postgresql-42.7.7.jar

COPY Main.java Main.java
RUN javac -cp "postgresql-42.7.7.jar" Main.java
RUN jar cfe app.jar Main Main.class

ENTRYPOINT ["java", "-cp", "app.jar:postgresql-42.7.7.jar", "Main"]
