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

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;


public class KafkaRiver extends AbstractRiverComponent implements River {

    private KafkaConsumer kafkaConsumer;
    private ElasticsearchProducer elasticsearchProducer;

    private Thread thread;

    @Inject
    protected KafkaRiver(RiverName riverName, RiverSettings riverSettings, Client client) {
        super(riverName, riverSettings);

        final RiverProperties riverProperties = new RiverProperties(riverName, riverSettings);
        kafkaConsumer = new KafkaConsumer(riverProperties, logger);
        elasticsearchProducer = new ElasticsearchProducer(client, riverProperties, logger);
    }

    @Override
    public void start() {

        try {
            logger.info("Starting Kafka Worker...");
            final KafkaWorker kafkaWorker = new KafkaWorker(kafkaConsumer, elasticsearchProducer, logger);

            thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "kafka-river").newThread(kafkaWorker);
            thread.start();
        } catch (Exception ex) {
            logger.error("Unexpected Error occurred", ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() {
        thread.interrupt();
    }
}
