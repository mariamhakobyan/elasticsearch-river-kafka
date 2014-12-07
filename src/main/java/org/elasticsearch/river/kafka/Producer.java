/*
 * Copyright 2013 Mariam Hakobyan
 * Copyright 2014 Linagora
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
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Set;

/**
 * An interface to abstract operations done from kafka messages
 */
public abstract class Producer {

    private final ESLogger logger = ESLoggerFactory.getLogger(Producer.class.getName());

    private Client client;
    protected KafkaConsumer kafkaConsumer;
    protected RiverConfig riverConfig;
    protected BulkProcessor bulkProcessor;

    public Producer(Client client, KafkaConsumer kafkaConsumer, RiverConfig riverConfig) {
        this.client = client;
        this.kafkaConsumer = kafkaConsumer;
        this.riverConfig = riverConfig;
        createBulkProcessor(kafkaConsumer);
    }

    public abstract void addMessagesToBulkProcessor(final Set<MessageAndMetadata> messageSet);


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

    public void closeBulkProcessor() {
        bulkProcessor.close();
    }
}
