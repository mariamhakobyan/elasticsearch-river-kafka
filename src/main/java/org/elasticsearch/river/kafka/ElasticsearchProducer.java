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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.util.Set;
import java.util.UUID;

/**
 * An Elasticsearch producer, which creates an index, mapping in the EL, and puts messages in EL.
 *
 * @author Mariam Hakobyan
 */
public class ElasticsearchProducer extends Producer {

    public ElasticsearchProducer(final Client client, final RiverConfig riverConfig, final KafkaConsumer kafkaConsumer) {
        super(client, kafkaConsumer, riverConfig);
    }

    /**
     * For the given messages creates index requests and adds them to the bulk processor queue, for
     * processing later when the size of bulk actions is reached.
     *
     * @param messageSet given set of messages
     */
    public void addMessagesToBulkProcessor(final Set<MessageAndMetadata> messageSet) {

        for (MessageAndMetadata messageAndMetadata : messageSet) {
            final byte[] messageBytes = (byte[]) messageAndMetadata.message();

            if (messageBytes == null || messageBytes.length == 0) return;

            try {
                // TODO - future improvement - support for protobuf messages
                final IndexRequest request = Requests.indexRequest(riverConfig.getIndexName()).
                        type(riverConfig.getTypeName()).
                        id(UUID.randomUUID().toString()).
                        source(XContentFactory.jsonBuilder()
                                .startObject()
                                .field("value", new String(messageBytes, "UTF-8"))
                                .endObject());

                bulkProcessor.add(request);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }
}
