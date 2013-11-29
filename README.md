Kafka River Plugin for ElasticSearch
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
