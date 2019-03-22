# Kafkascraper

Scrape a kafka topic for messages.

Topic, cluster URL, start/end time and a simple filter can 
be configured via command line parameters.

Build a fat jar via
```
./mvnw clean package
```
(jar is located under `target`)

Run the fat jar (requires JRE 8 or above)
```
java -jar target/kafkascraper-1.0-SNAPSHOT.jar --help
```

Minimal command line parameters are
```
java -jar target/kafkascraper-1.0-SNAPSHOT.jar \
    --topic my-topic \
    --bootstrap localhost:9092
```

A complete example would be
```
java -jar target/kafkascraper-1.0-SNAPSHOT.jar \
    --topic my-topic \
    --bootstrap localhost:9092 \
    --start 2019-01-01T14:00:00Z \
    --end 2019-01-01T15:00:00Z \
    --filter ".*what to find.*" \
    --progress 1000
```
which prints all messages between the given times whose key or value 
match the given regex. After every 1000 messages without a match (for the regex)
a dot (".") is printed.

### Known issues

* If no messages are received (before filtering) process waits forever.
* No checks if cluster exists are performed.
* Only strings (includes JSON) are supported for keys and values, anything else is printed as binary.