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
import org.elasticsearch.common.logging.ESLogger;


public class KafkaWorker implements Runnable {

    private KafkaConsumer kafkaConsumer;
    private ElasticsearchProducer elasticsearchProducer;
    private KafkaProperties kafkaProperties;

    private ESLogger logger;


    public KafkaWorker(KafkaConsumer kafkaConsumer, ElasticsearchProducer elasticsearchProducer, ESLogger logger) {
        this.kafkaConsumer = kafkaConsumer;
        this.kafkaProperties = kafkaConsumer.getKafkaProperties();
        this.elasticsearchProducer = elasticsearchProducer;
        this.logger = logger;
    }

    @Override
    public void run() {

        logger.info("KafkaWorker started...");

        try {
            while (true) {

                // Read Kafka messages with KafkaConsumer
                ByteBufferMessageSet messages = kafkaConsumer.readMessagesFromKafka();
                Long currentOffset = kafkaConsumer.getCurrentOffset();

                if (messages.validBytes() > 0) {
                    // Insert Kafka messages into elasticsearch
                    elasticsearchProducer.writeMessagesToElasticSearch(messages);

                    currentOffset += messages.validBytes();
                    kafkaConsumer.setCurrentOffset(currentOffset);
                } else {

                    logger.debug("No messages received from Kafka for topic={}, partition={}",
                            kafkaProperties.getTopic(), kafkaProperties.getPartition());

                    Thread.sleep(1000);
                }
            }
        } catch (Exception ex) {
            logger.error("Unexpected error occurred...");

            throw  new RuntimeException(ex);
        }
    }


}
