FROM eclipse-temurin:17 AS build

WORKDIR /ph-ee-connector-tnm

COPY . .

RUN if ls build/libs/ph-ee-connector-tnm-*.jar 1>  /dev/null 2>&1 ; then echo "Using Already built JAR";  \
    else ./gradlew bootJar ; fi

FROM eclipse-temurin:17

WORKDIR /app

COPY --from=build /ph-ee-connector-tnm/build/libs/ph-ee-connector-tnm*.jar ph-ee-connector-tnm.jar
COPY --from=build /ph-ee-connector-tnm/config/elastic/elastic-apm-agent-1.54.0.jar /app/config/elastic/elastic-apm-agent.jar

EXPOSE 5000

ENTRYPOINT ["java", "-jar", "/app/ph-ee-connector-tnm.jar"]
