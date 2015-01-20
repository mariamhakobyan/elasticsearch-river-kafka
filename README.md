Kafka River Plugin for ElasticSearch
=========

The Kafka River plugin allows you to read messages from Kafka and index bulked messages into elasticsearch.
The bulk size (the number of messages to be indexed in one request) and concurrent request number is configurable.
The Kafka River also supports consuming messages from multiple Kafka brokers and multiple partitions. 

The plugin uses the latest Kafka and Elasticsearch version.
 * Kafka version 0.8.1.1
 * Elasticsearch version 1.4.0

The plugin is periodically updated, if there are newer versions of any dependencies.
It is available in the [ElasticSearch's official website](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/modules-plugins.html).

Setup
==========

1. Install Kafka if you are working on local environment (See [Apache Kafka Quick Start Guide](http://kafka.apache.org/07/quickstart.html)  for instructions on how to Download and Build.)

2. Install the plugin 

```sh
cd $ELASTICSEARCH_HOME
.bin/plugin -install <plugin-name> -url https://github.com/mariamhakobyan/elasticsearch-river-kafka/releases/download/v1.2.1/elasticsearch-river-kafka-1.2.1-plugin.zip
```
*Example:*
```sh
cd $ELASTICSEARCH_HOME
.bin/plugin -install kafka-river -url https://github.com/mariamhakobyan/elasticsearch-river-kafka/releases/download/v1.2.1/elasticsearch-river-kafka-1.2.1-plugin.zip
```

If it doesn't work, clone git repository and build plugin manually.
* Build the plugin - it will create a zip file here: $PROJECT-PATH/target/elasticsearch-river-kafka-1.2.1-SNAPSHOT-plugin.zip
* Install the plugin from target into elasticsearch
 
```sh
cd $ELASTICSEARCH_HOME
.bin/plugin --install <plugin-name> --url file:////$PLUGIN-PATH/elasticsearch-river-kafka-1.2.1-SNAPSHOT-plugin.zip
```

Update installed plugin

```sh
cd $ELASTICSEARCH_HOME
./bin/plugin -remove <plugin-name>
./bin/plugin -url file:/$PLUGIN_PATH -install <plugin-name>
```

Configuration
=========

To deploy Kafka river into Elasticsearch as a plugin, execute:

```json
curl -XPUT 'localhost:9200/_river/<river-name>/_meta' -d '
{
     "type" : "kafka",
     "kafka" : {
        "zookeeper.connect" : <zookeeper.connect>, 
        "zookeeper.connection.timeout.ms" : <zookeeper.connection.timeout.ms>,
        "topic" : <topic.name>,
        "message.type" : <message.type>
    },
    "index" : {
        "index" : <index.name>,
        "type" : <mapping.type.name>,
        "bulk.size" : <bulk.size>,
        "flush.interval" : <flush.interval>,
        "concurrent.requests" : <concurrent.requests>,
        "action.type" : <action.type>
     }
 }'
 ```
 * ***NOTE***: Type "kafka" is required and must not be changed. It corresponds the type, given in the source code, by which elasticsearch is able to associate created river with the installed plugin.
 
 *Example:*

 ```json
 curl -XPUT 'localhost:9200/_river/kafka-river/_meta' -d '
 {
      "type" : "kafka",
      "kafka" : {
         "zookeeper.connect" : "localhost", 
         "zookeeper.connection.timeout.ms" : 10000,
         "topic" : "river",
         "message.type" : "json"
     },
     "index" : {
         "index" : "kafka-index",
         "type" : "status",
         "bulk.size" : 100,
         "flush.interval" : 720000,
         "concurrent.requests" : 1,
         "action.type" : "index"
      }
  }'
  ```
 
The detailed description of each parameter:
 
* `river-name` (required) - The river name to be created in elasticsearch.
* `zookeeper.connect` (optional) - Zookeeper server host. Default is: `localhost`
* `zookeeper.connection.timeout.ms` (optional) - Zookeeper server connection timeout in milliseconds. Default is: `10000`
* `topic` (optional) - The name of the topic where you want to send Kafka message. Default is: `elasticsearch-river-kafka`
* `message.type` (optional) - The kafka message type, which then will be inserted into ES keeping the same type. Default is: `json`. The following options are available: 
   - `json` : Inserts json message into ES separating each json property into ES document property.
   *example:*
      ```json
       "_source": {
          "name": "John",
          "age": 28
       }
      ```
   
   - `string` : Inserts string message into ES as a documet, where the key name is `value`, and the value is the received message.
   *example:*
    ```json
     "_source": {
          "value": "received text message"
     }
    ```
   
* `index` (optional) - The name of elasticsearch index. Default is: `kafka-index`
* `type` (optional) - The mapping type of elasticsearch index. Default is: `status`
* `bulk.size` (optional) - The number of messages to be bulk indexed into elasticsearch. Default is: `100`
* `flush.interval` (optional) - The number of seconds after which any remaining messages get flushed to elasticsearch, even if the number of messages has not reached. Default is: `720000` (12 hours)
* `concurrent.requests` (optional) - The number of concurrent requests of indexing that will be allowed. A value of 0 means that only a single request will be allowed to be executed. A value of 1 means 1 concurrent request is allowed to be executed while accumulating new bulk requests. Default is: `1`
* `action.type` (optional) - The action type against how the messages should be processed. Default is: `index`. The following options are available:
   - `index` : Creates documents in ES with the `value` field set to the received message.
   - `delete` : Deletes documents from ES based on `id` field set in the received message.
   - `raw.execute` : Execute incoming messages as a raw query.
   

To delete the existing river, execute:
 
```json
curl -XDELETE 'localhost:9200/_river/<river-name>/'
``` 

*Example:*
```json
curl -XDELETE 'localhost:9200/_river/kafka-river/'
``` 


To see the indexed data:

```json
curl -XGET 'localhost:9200/kafka-index/_search?pretty=1'
```

To delete the index:
```json
curl -XDELETE 'localhost:9200/kafka-index'
```

Kafka Consumer details
=========

Currently Consumer Group Java API (high level api) is used to create the Kafka Consumer, which keeps track of offset automatically. This enables the River to read kafka messages from multiple brokers and multiple partitions.


License
=========
Copyright (C) 2014 Mariam Hakobyan. See `LICENSE`


Contributing
============
1. Fork the repository on Github
2. Create a named feature branch
3. Develop your changes in a branch
4. Write tests for your change (if applicable)
5. Ensure all the tests are passing
6. Submit a Pull Request using Github


