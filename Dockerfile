FROM gradle:8.13.0-jdk17-alpine AS build
COPY --chown=gradle:gradle . /home/gradle/src
WORKDIR /home/gradle/src
RUN gradle shadowJar --no-daemon 

FROM eclipse-temurin:17
WORKDIR app
COPY --from=build /home/gradle/src/build/libs/*.jar /app/app-all.jar
ENTRYPOINT ["java", "-jar", "app-all.jar"]