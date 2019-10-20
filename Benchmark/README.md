
## Build and run
To download dependencies and build the executable jar-file, do:
```
mvn clean dependency:copy-dependencies package
```

Then run the benchmark with:
```
java -jar target/benchmark-1.0-SNAPSHOT.jar "path to benchmark config file"
```
or use the following to generate the default config file and then exit:
```
java -jar target/benchmark-1.0-SNAPSHOT.jar --default-config
```