
# Dataverse-superset

Integration tool for Dataverse for data visualization using Apache Superset

## Basic setup

Requires Java 11+

1. Copy `application.yaml.sample` to `application.yaml` and change the relevant settings (mainly the database and Superset access configs).
2. Build the app with `./gradlew build`
3. Start the app with `./gradlew bootRun` or `java -jar build/libs/dataverse-superset-0.0.1-SNAPSHOT.jar`.
   The service should be available at `http://127.0.0.1:4480/dataverse`.
4. To register as Dataverse extension, use the `dataverse-superset.json.sample` example file (change the `toolUrl` value if necessary).
   Send it with the command like this: `curl -X POST -H 'Content-type: application/json' http://127.0.0.1/api/admin/externalTools --upload-file dataverse-superset.json`
