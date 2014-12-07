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
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.ChannelBufferBytesReference;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.netty.buffer.ByteBufferBackedChannelBuffer;

import java.nio.ByteBuffer;
import java.util.Set;

/**
 * This producer execute bulk requests.
 */
public class BulkProducer extends Producer {

    private final ESLogger logger = ESLoggerFactory.getLogger(BulkProducer.class.getName());

    public BulkProducer(final Client client, final RiverConfig riverConfig, final KafkaConsumer kafkaConsumer) {
        super(client, kafkaConsumer, riverConfig);
    }

    public void addMessagesToBulkProcessor(final Set<MessageAndMetadata> messageSet) {
        for(MessageAndMetadata messageAndMetadata : messageSet) {
            final byte[] messageBytes = (byte[])  messageAndMetadata.message();
            try {
                ByteBuffer byteBuffer = ByteBuffer.wrap(messageBytes);
                bulkProcessor.add(
                        new ChannelBufferBytesReference(new ByteBufferBackedChannelBuffer(byteBuffer)),
                        false,
                        riverConfig.getIndexName(),
                        riverConfig.getTypeName()
                );
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("Could not index that");
            } finally {
                kafkaConsumer.getConsumerConnector().commitOffsets();
            }

        }
    }


}
