package org.elasticsearch.river.kafka;

import kafka.message.MessageAndMetadata;

import java.util.Set;

/**
 * Created by benwa on 12/2/14.
 */
public interface Producer {

    public void addMessagesToBulkProcessor(final Set<MessageAndMetadata> messageSet);

    public void closeBulkProcessor();
}
