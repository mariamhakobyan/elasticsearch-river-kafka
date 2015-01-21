/*
 * Copyright 2014 Mariam Hakobyan
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

import java.util.Map;
import java.util.Set;

import kafka.message.MessageAndMetadata;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

/**
 * Producer to delete documents. Creates delete document requests, which are executed with Bulk API.
 *
 * @author Mariam Hakobyan
 */
public class DeleteDocumentProducer extends ElasticSearchProducer {

    private static final ESLogger logger = ESLoggerFactory.getLogger(DeleteDocumentProducer.class.getName());
    
    public DeleteDocumentProducer(Client client, RiverConfig riverConfig, KafkaConsumer kafkaConsumer) {
        super(client, riverConfig, kafkaConsumer);
    }

    /**
     * For the given messages creates delete document requests and adds them to the bulk processor queue, for
     * processing later when the size of bulk actions is reached.
     *
     * @param messageSet given set of messages
     */
    public void addMessagesToBulkProcessor(final Set<MessageAndMetadata> messageSet) {

        for (MessageAndMetadata messageAndMetadata : messageSet) {
            final byte[] messageBytes = (byte[]) messageAndMetadata.message();

            if (messageBytes == null || messageBytes.length == 0) return;

            try {
                final Map<String, Object> messageMap = reader.readValue(messageBytes);

                if(messageMap.containsKey("id")) {
                    String id = (String)messageMap.get("id");

                    final DeleteRequest request = Requests.deleteRequest(riverConfig.getIndexName()).
                            type(riverConfig.getTypeName()).
                            id(id);

                    bulkProcessor.add(request);
                } else {
                    throw new IllegalArgumentException("No id provided in a message to delete a document from EL.");
                }
            } catch (Exception ex) {
                logger.error("addMessagesToBulkProcessor exception", ex);
            }
        }
    }
}
