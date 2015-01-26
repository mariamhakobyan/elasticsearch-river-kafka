package org.elasticsearch.river.kafka;

import kafka.message.MessageAndMetadata;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.ChannelBufferBytesReference;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.netty.buffer.ByteBufferBackedChannelBuffer;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

/**
 * This producer takes  a JSon representing an ElasticSearch request.
 *
 * It was added to support non bulkable operations that we would like to manage threw a Kafka queue. For instance delete by query.
 *
 * @author Benoit Tellier from Linagora
 */
public class GenericRequestProducer extends ElasticSearchProducer {

    public final static String TYPE = "type";
    public final static String QUERY = "query";
    public final static String BULK = "bulk";
    public final static String DELETE_BY_QUERY = "delete_by_query";
    public final static String CONTENT = "content";

    private static final ESLogger logger = ESLoggerFactory.getLogger(GenericRequestProducer.class.getName());

    public GenericRequestProducer (Client client, RiverConfig riverConfig, KafkaConsumer kafkaConsumer) {
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
                XContentParser parser = JsonXContent.jsonXContent.createParser(messageBytes);
                Map<String, Object> keys = parser.mapAndClose();
                String type = (String) keys.get(TYPE);
                if (type.equalsIgnoreCase(BULK)) {
                    String content = (String) keys.get(CONTENT);
                    ByteBuffer byteBuffer = ByteBuffer.wrap(content.getBytes());
                    bulkProcessor.add(
                            new ChannelBufferBytesReference(new ByteBufferBackedChannelBuffer(byteBuffer)),
                            false,
                            riverConfig.getIndexName(),
                            riverConfig.getTypeName()
                    );
                } else if (type.equalsIgnoreCase(DELETE_BY_QUERY) ) {
                    String queryString = (String) keys.get(QUERY);
                    client.prepareDeleteByQuery().setSource(queryString).execute();
                } else {
                    logger.warn("Unknown TYPE provided");
                }
            } catch (Exception ex) {
                logger.error("Exception", ex);
                ex.printStackTrace();
            }
        }
    }
}
