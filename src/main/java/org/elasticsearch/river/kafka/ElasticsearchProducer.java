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

import kafka.message.MessageAndMetadata;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.util.Set;
import java.util.UUID;

/**
 * An Elasticsearch producer, which creates an index, mapping in the EL, and puts messages in EL.
 *
 * @author Mariam Hakobyan
 */
public class ElasticsearchProducer {

    private final ESLogger logger = ESLoggerFactory.getLogger(ElasticsearchProducer.class.getName());

    private Client client;
    private BulkProcessor bulkProcessor;

    private RiverConfig riverConfig;

    public ElasticsearchProducer(final Client client, final RiverConfig riverConfig, final KafkaConsumer kafkaConsumer) {
        this.client = client;
        this.riverConfig = riverConfig;

        createBulkProcessor(kafkaConsumer);
    }

    private void createBulkProcessor(final KafkaConsumer kafkaConsumer) {
        bulkProcessor = BulkProcessor.builder(client,
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId, BulkRequest request) {
                        logger.info("Going to execute bulk request composed of {} actions.", request.numberOfActions());
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                        logger.info("Executed bulk composed of {} actions.", request.numberOfActions());

                        // Commit the kafka messages offset, only when messages have been successfully
                        // inserted into elasticsearch
                        kafkaConsumer.getConsumerConnector().commitOffsets();
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                        logger.warn("Error executing bulk.", failure);
                    }
                })
                .setBulkActions(riverConfig.getBulkSize())
                .setFlushInterval(TimeValue.timeValueHours(12))
                .setConcurrentRequests(riverConfig.getConcurrentRequests())
                .build();
    }

    /**
     * For the given messages creates index requests and adds them to the bulk processor queue, for
     * processing later when the size of bulk actions is reached.
     *
     * @param messageSet given set of messages
     */
    public void addMessagesToBulkProcessor(final Set<MessageAndMetadata> messageSet) {

        String msg;

        for (MessageAndMetadata messageAndMetadata : messageSet) {
            final byte[] messageBytes = (byte[]) messageAndMetadata.message();

            if (messageBytes == null || messageBytes.length == 0) return;

            try {
                // TODO - future improvement - support for protobuf messages

                // Treat the Kafka message as JSON or a String
                if (riverConfig.getKafkaMessageJson()) {
                    msg = new String(messageBytes, "UTF-8");
                } else {
                    msg = (String) XContentFactory.jsonBuilder()
                        .startObject()
                        .field("value", new String(messageBytes, "UTF-8"))
                        .endObject()
                        .string();
                }
                final IndexRequest request = Requests.indexRequest(riverConfig.getIndexName()).
                        type(riverConfig.getTypeName()).
                        id(UUID.randomUUID().toString()).
                        source(msg);

                bulkProcessor.add(request);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    public void closeBulkProcessor() {
        bulkProcessor.close();
    }
}
