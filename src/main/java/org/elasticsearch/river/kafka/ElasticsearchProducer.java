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
import kafka.message.MessageAndMetadata;
import kafka.message.MessageAndOffset;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.nio.ByteBuffer;
import java.util.Random;

/**
 * An Elasticsearch producer, which creates an index, mapping in the EL, and puts messages in EL.
 *
 * @author Mariam Hakobyan
 */
public class ElasticsearchProducer {

    private Client client;
    private ESLogger logger;
    private RiverProperties riverProperties;

    public ElasticsearchProducer(Client client, RiverProperties riverProperties, ESLogger logger) {
        this.client = client;
        this.riverProperties = riverProperties;
        this.logger = logger;
    }

    public void writeMessagesToElasticSearch(ByteBufferMessageSet messages) {
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

        for (MessageAndOffset message : messages) {
            byte[] payload = getMessage(message.message());

            // TODO Can be parsed to a protobuf message
            try {
                prepareBulkRequestBuilder(payload, bulkRequestBuilder);
                executeBulkRequestBuilder(bulkRequestBuilder);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

//    public void writeMessagesToElasticSearch(byte[] message) {
//        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
//        try {
//            prepareBulkRequestBuilder(message, bulkRequestBuilder);
//            executeBulkRequestBuilder(bulkRequestBuilder);
//        } catch (Exception ex) {
//            ex.printStackTrace();
//        }
//    }

    private void prepareBulkRequestBuilder(byte[] message, BulkRequestBuilder bulkRequestBuilder) throws Exception {
        if (message == null || message.length == 0) return;

//        bulkRequestBuilder.add(client.prepareIndex("_river", "kafka-river").setSource(message));

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

    public void writeMessagesToElasticSearch(MessageAndMetadata messageAndMetadata) {
        byte[] messageBytes = (byte []) messageAndMetadata.message();

        if (messageBytes.length == 0) return;

        String indexName = riverProperties.getIndexName();
        String typeName = riverProperties.getTypeName();

        try {
            IndexResponse response = client.prepareIndex(indexName, typeName, String.valueOf(new Random().nextInt(100)))
                    .setSource(XContentFactory.jsonBuilder()
                                    .startObject()
                                    .field("msg", new String(messageBytes, "UTF-8"))
                                    .endObject()
                    )
                    .execute()
                    .actionGet();
        } catch (Exception e) {
            logger.debug("failed to apply default mapping [{}]/[{}], disabling river...", e, indexName, typeName);
        }

    }

    public void indexSingleMessages(byte[] message) {
//        // 1 - Create an index
//        try {
//            client.admin().indices().prepareCreate(riverProperties.getIndexName()).execute().actionGet();
//        } catch (Exception e) {
//            if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
//                // that's fine
//            } else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
//                // ok, not recovered yet..., lets start indexing and hope we recover by the first bulk
//                // TODO: a smarter logic can be to register for cluster event listener here, and only start sampling when the block is removed...
//            } else {
//                logger.warn("failed to create index [{}], disabling river...", e, riverProperties.getIndexName());
//                return;
//            }
//        }

        // 2 - Create mapping type
        String indexName = riverProperties.getIndexName();
        String typeName = riverProperties.getTypeName();

//        try {
//            String mapping = XContentFactory.jsonBuilder()
//                    .startObject()
//                    .field("msg", new String(message, "UTF-8"))
//                    .endObject().toString();
//
////            .startObject("user").startObject("properties").startObject("screen_name").field("type", "string").field("index", "not_analyzed").endObject().endObject().endObject()
//
//            logger.debug("Applying default mapping for [{}]/[{}]: {}", indexName, typeName, mapping);
//
//            client.admin().indices().
//                    preparePutMapping(riverProperties.getIndexName()).
//                    setType(riverProperties.getTypeName()).
//                    setSource(mapping).
//                    execute().actionGet();
//        } catch (Exception e) {
//            logger.debug("failed to apply default mapping [{}]/[{}], disabling river...", e, indexName, typeName);
//        }

        try {
            IndexResponse response = client.prepareIndex(indexName, typeName, String.valueOf(new Random().nextInt(100)))
                    .setSource(XContentFactory.jsonBuilder()
                                    .startObject()
                                    .field("msg", new String(message, "UTF-8"))
                                    .endObject()
                    )
                    .execute()
                    .actionGet();
        } catch (Exception e) {
            logger.debug("failed to apply default mapping [{}]/[{}], disabling river...", e, indexName, typeName);
        }
    }

    private byte[] getMessage(Message message) {
        ByteBuffer payload = message.payload();
        byte[] bytes = new byte[payload.limit()]; //payload.remaining()
        payload.get(bytes);
        return bytes;
    }

}
