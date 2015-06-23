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

import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Random;
import java.util.Set;


/**
 * The worker thread, which does the actual job of consuming messages from kafka and passing those to
 * Elastic Search producer - {@link ElasticSearchProducer} to index.
 * Behind the scenes of kafka high level API, the worker will read the messages from different kafka brokers and
 * partitions.
 */
public class KafkaWorker implements Runnable {

    private KafkaConsumer kafkaConsumer;
    private ElasticSearchProducer elasticsearchProducer;
    private RiverConfig riverConfig;
    private Stats stats;

    private volatile boolean consume = false;

    private static final ESLogger logger = ESLoggerFactory.getLogger(KafkaWorker.class.getName());

    /**
     * For randomly selecting the kafka partition.
     */
    private Random random = new Random();


    public KafkaWorker(final KafkaConsumer kafkaConsumer,
                       final ElasticSearchProducer elasticsearchProducer,
                       final RiverConfig riverConfig,
                       final Stats stats) {
        this.kafkaConsumer = kafkaConsumer;
        this.elasticsearchProducer = elasticsearchProducer;
        this.riverConfig = riverConfig;
        this.stats = stats;
    }

    @Override
    public void run() {

        logger.debug("Index: {}: Kafka worker started...", riverConfig.getIndexName());

        if (consume) {
            logger.debug("Index: {}: Consumer is already running, new one will not be started...", riverConfig.getIndexName());
            return;
        }

        consume = true;
        try {
            logger.debug("Index: {}: Kafka consumer started...", riverConfig.getIndexName());

            while (consume) {
                KafkaStream stream = chooseRandomStream(kafkaConsumer.getStreams());
                consumeMessagesAndAddToBulkProcessor(stream);
            }
        } finally {
            logger.debug("Index: {}: Kafka consumer has stopped...", riverConfig.getIndexName());
            consume = false;
        }
    }

    /**
     * Consumes the messages from the partition via specified stream.
     */
    private void consumeMessagesAndAddToBulkProcessor(final KafkaStream stream) {

        try {
            // by default it waits forever for message, but there is timeout configured
            final ConsumerIterator<byte[], byte[]> consumerIterator = stream.iterator();

            // Consume all the messages of the stream (partition)
            while (consumerIterator.hasNext() && consume) {

                final MessageAndMetadata messageAndMetadata = consumerIterator.next();
                logMessage(messageAndMetadata);

                elasticsearchProducer.addMessagesToBulkProcessor(messageAndMetadata);

                // StatsD reporting
                stats.messagesReceived.incrementAndGet();
                stats.lastCommitOffsetByPartitionId.put(messageAndMetadata.partition(), messageAndMetadata.offset());
            }
        } catch (ConsumerTimeoutException ex) {
            logger.debug("Nothing to be consumed for now. Consume flag is: {}", consume);
        }
    }

    /**
     * Chooses a random stream to consume messages from, from the given list of all streams.
     *
     * @return randomly chosen stream
     */
    private KafkaStream chooseRandomStream(final List<KafkaStream<byte[], byte[]>> streams) {
        final int streamNumber = random.nextInt(streams.size());
        return streams.get(streamNumber);
    }

    /**
     * Logs consumed kafka messages to the log.
     */
    private void logMessage(final MessageAndMetadata messageAndMetadata) {
        final byte[] messageBytes = (byte[]) messageAndMetadata.message();

        try {
            final String message = new String(messageBytes, "UTF-8");

            logger.debug("Index: {}: Message received: {}", riverConfig.getIndexName(), message);
        } catch (UnsupportedEncodingException e) {
            logger.debug("The UTF-8 charset is not supported for the kafka message.");
        }
    }
}
