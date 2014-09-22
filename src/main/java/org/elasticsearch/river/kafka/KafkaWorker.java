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
import org.elasticsearch.common.logging.ESLogger;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Random;


/**
 *  The worker thread, which does the actual job of consuming messages from kafka and passing those to
 *  Elastic Search producer - {@link org.elasticsearch.river.kafka.ElasticsearchProducer} to index.
 *  Behind the scenes of kafka high level API, the worker will read the messages from different kafka brokers and
 *  partitions.
 */
public class KafkaWorker implements Runnable {

    private KafkaConsumer kafkaConsumer;
    private ElasticsearchProducer elasticsearchProducer;
    private ESLogger logger;

    private volatile boolean consume = false;


    /** For randomly selecting the partition of a kafka partition. */
    private Random random = new Random();


    public KafkaWorker(KafkaConsumer kafkaConsumer, ElasticsearchProducer elasticsearchProducer, ESLogger logger) {
        this.kafkaConsumer = kafkaConsumer;
        this.elasticsearchProducer = elasticsearchProducer;
        this.logger = logger;
    }

    @Override
    public void run() {

        logger.info("Kafka worker started...");

        if (consume) {
            logger.info("Consumer is already running, new one will not be started...");
            return;
        }

        consume = true;
        try {
            logger.info("Kafka consumer started!");

            while (consume) {
                KafkaStream stream = chooseRandomStream(kafkaConsumer.getStreams());
                consumePartitionMessages(stream);
            }
        } finally {
            logger.info("Kafka consumer has stopped!");
            consume = false;
        }
    }

    /**
     * Consumes the messages from the partition via specified stream.
     */
    private void consumePartitionMessages(KafkaStream stream) {

        try {
            // by default it waits forever for message, but there is timeout configured
            final ConsumerIterator<byte[], byte[]> consumerIterator = stream.iterator();

            // Consume all the messages of the stream (partition)
            while (consumerIterator.hasNext() && consume) {

                final MessageAndMetadata messageAndMetadata = consumerIterator.next();

                logMessages(messageAndMetadata);
                elasticsearchProducer.writeMessagesToElasticSearch(messageAndMetadata);

                kafkaConsumer.getConsumer().commitOffsets();
            }
        } catch (ConsumerTimeoutException ex) {
            logger.info("Nothing to be consumed for now. Consume flag is: " + consume);
        }
    }

    /**
     * Chooses a random stream to consume messages from, from the given list of all streams.
     *
     * @return  randomly choosen stream
     */
    private KafkaStream chooseRandomStream(List<KafkaStream<byte[], byte[]>> streams) {
        final int streamNumber = random.nextInt(streams.size());

        logger.info("Selected stream " + streamNumber + " out of  " + streams.size() + " from TOPIC: " + kafkaConsumer.getRiverProperties().getTopic());

        return streams.get(streamNumber);
    }

    /**
     * Logs consumed kafka messages to the log.
     */
    private void logMessages(MessageAndMetadata messageAndMetadata) {
        byte[] messageBytes = (byte []) messageAndMetadata.message();

        try {
            String message = new String(messageBytes, "UTF-8");

            logger.info(message);
        } catch (UnsupportedEncodingException e) {
            logger.info("The UTF-8 charset is not supported for the kafka message");
        }
    }
}
