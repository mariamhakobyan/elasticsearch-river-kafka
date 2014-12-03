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
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import java.util.Set;

/**
 * This producer execute bulk requests.
 */
public class BulkProducer implements Producer {
    private final ESLogger logger = ESLoggerFactory.getLogger(BulkProducer.class.getName());

    private Client client;
    private KafkaConsumer kafkaConsumer;
    private RiverConfig riverConfig;

    public BulkProducer(final Client client, final RiverConfig riverConfig, final KafkaConsumer kafkaConsumer) {
        this.client = client;
        this.kafkaConsumer = kafkaConsumer;
        this.riverConfig = riverConfig;
    }

    public void addMessagesToBulkProcessor(final Set<MessageAndMetadata> messageSet) {
        logger.info("In bulk processor");
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk().setReplicationType(ReplicationType.DEFAULT);
        for(MessageAndMetadata messageAndMetadata : messageSet) {
            final byte[] messageBytes = (byte[])  messageAndMetadata.message();
            try {
                bulkRequestBuilder.add(messageBytes, 0, messageBytes.length, false, riverConfig.getIndexName(), riverConfig.getTypeName());
                BulkResponse response = bulkRequestBuilder.execute().actionGet();
                if (response.hasFailures()) {
                    logger.warn("failed to execute" + response.buildFailureMessage());
                }
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("Could not index that");
            } finally {
                kafkaConsumer.getConsumerConnector().commitOffsets();
            }

        }
    }

    public void closeBulkProcessor() {

    }


}
