Kafka River Plugin for ElasticSearch
=========

To install the river

* Build the river plugin project
* Install the plugin from target into elasticsearch plugins

```javascript
bin/plugin --url file:////path-to-jar/elasticsearch-river-kafka-1.0-SNAPSHOT.jar --install kafka-river
```

=========
To start the river

```json
curl -XPUT 'localhost:9200/_river/my_kafka_river/_meta' -d '{
    "type" : "kafka",
    "kafka" : {
        "brokerHost" : "localhost", 
        "brokerPort" : 9092,
        "topic" : "test_topic",
        "partition" : 0

    }
}'
```
