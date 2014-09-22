 * Install the plugin from target into elasticsearch plugins
 
 ```javascript
bin/plugin --url file:////path-to-zip-file/elasticsearch-river-kafka-1.0.0-SNAPSHOT-plugin.zip --install kafka-river
 ```
 
 =========
To delete the existing river:
 
 ```json
curl -XDELETE 'localhost:9200/_river/kafka-river/'
```

To start the river:

```json
curl -XPUT 'localhost:9200/_river/kafka-river/_meta' -d '
{
     "type" : "kafka",
     "kafka" : {
        "zookeeper.connect" : "localhost", 
        "zookeeper.connection.timeout.ms" : 10000,
        "topic" : "river"
    },
    "index" : {
        "index" : "kafka-index",
        "type" : "messages"
     }
 }'
 ```

To see the indexed data:

```json
curl -XGET 'localhost:9200/kafka-index/_search?pretty=1'
```
