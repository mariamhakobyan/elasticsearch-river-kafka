/*
 * Copyright 2013 Mariam Hakobyan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.river.kafka;

import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;

import java.nio.ByteBuffer;


public class ElasticsearchProducer {

    private Client client;
    private ESLogger logger;
    private KafkaProperties kafkaProperties;

    public ElasticsearchProducer(Client client, KafkaProperties kafkaProperties, ESLogger logger) {
        this.client = client;
        this.kafkaProperties = kafkaProperties;
        this.logger = logger;
    }

    public void writeMessagesToElasticSearch(ByteBufferMessageSet messages) {
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

        for (MessageAndOffset message : messages) {
            byte[] body = getMessage(message.message());
            try {
                prepareBulkRequestBuilder(body, bulkRequestBuilder);
                executeBulkRequestBuilder(bulkRequestBuilder);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    private void prepareBulkRequestBuilder(byte[] message, BulkRequestBuilder bulkRequestBuilder) throws Exception {
        if (message == null) return;
                                                     // ??                           ???
        bulkRequestBuilder.add(client.prepareIndex(kafkaProperties.getTopic(), kafkaProperties.getTopic()).setSource(message)); // json type

        //bulkRequestBuilder.add(client.prepareIndex().setSource(message)); // json type without specifying index and type


        // Raw message type
//        bulkRequestBuilder.add(message, 0, message.length, false);
    }

    private void executeBulkRequestBuilder(BulkRequestBuilder bulkRequestBuilder) {

        if(bulkRequestBuilder.numberOfActions() == 0)
            return;

        BulkResponse response = bulkRequestBuilder.execute().actionGet();
        if (response.hasFailures()) {
            logger.warn("failed to execute" + response.buildFailureMessage());
        }
    }

    private byte[] getMessage(Message message) {
        ByteBuffer buffer = message.payload();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

}
