### Requirements
- JDK v1.8 or above
- Maven v3.5 or above

### Compliation

```
mvn clean install
```

### Running an experimet

```
mvn exec:java -Dexec.mainClass="<file name>" -Dexec.args="<number of owners> <number of rows> <number of threads>"

Example: 
mvn exec:java -Dexec.mainClass="PSICloud" -Dexec.args="10 20000000 8"
```

