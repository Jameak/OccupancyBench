## Build and run
To download dependencies and build the executable jar-file, do:
```
mvn clean dependency:copy-dependencies package
```

Then run the seed generator with:
```
java -jar target/seed-generator-1.0-SNAPSHOT.jar --floors 5 --sensors-per-floor 25
```
By default, 7 days of data will be written to the current working directory. For a full list of generator arguments, use the `--help` argument.