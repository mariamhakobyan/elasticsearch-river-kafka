package org.elasticsearch.river.kafka;

import kafka.message.MessageAndMetadata;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import java.util.Set;

public class BulkProducer implements Producer {
    private final ESLogger logger = ESLoggerFactory.getLogger(BulkProducer.class.getName());

    private Client client;
    private RiverConfig riverConfig;
    private KafkaConsumer kafkaConsumer;

    public BulkProducer(final Client client, final RiverConfig riverConfig, final KafkaConsumer kafkaConsumer) {
        this.client = client;
        this.riverConfig = riverConfig;
        this.kafkaConsumer = kafkaConsumer;
    }

    public void addMessagesToBulkProcessor(final Set<MessageAndMetadata> messageSet) {
        logger.info("In bulk processor");
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk().setReplicationType(ReplicationType.DEFAULT);
        for(MessageAndMetadata messageAndMetadata : messageSet) {
            final byte[] messageBytes = (byte[])  messageAndMetadata.message();
            try {
                bulkRequestBuilder.add(messageBytes, 0, messageBytes.length, false);
            } catch (Exception e) {
                kafkaConsumer.getConsumerConnector().commitOffsets();
                e.printStackTrace();
                logger.error("Could not index that");
                continue;
            }
            BulkResponse response = bulkRequestBuilder.execute().actionGet();
            if (response.hasFailures()) {
                logger.warn("failed to execute" + response.buildFailureMessage());
            }
        }
    }

    public void closeBulkProcessor() {

    }


}
