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

import java.util.Timer;
import java.util.TimerTask;

/**
 * This is the actual river implementation, which starts a thread to read messages from kafka and put them into elasticsearch.
 */
public class KafkaRiver extends AbstractRiverComponent implements River {

    private KafkaConsumer kafkaConsumer;
    private ElasticSearchProducer elasticsearchProducer;
    private RiverConfig riverConfig;
    
    private Stats stats;
    private StatsAgent statsAgent;
    private Timer timer;
    
    private Thread thread;

    @Inject
    protected KafkaRiver(final RiverName riverName, final RiverSettings riverSettings, final Client client) {
        super(riverName, riverSettings);

        riverConfig = new RiverConfig(riverName, riverSettings);
        kafkaConsumer = new KafkaConsumer(riverConfig);
        stats = new Stats();
        
        if(null != riverConfig.getStatsdHost()) {
            logger.debug("Found statsd configuration. Starting client (prefix={}, host={}, port={}, interval={})",
                    riverConfig.getStatsdPrefix(), riverConfig.getStatsdHost(), riverConfig.getStatsdPort(),
                    riverConfig.getStatsdIntervalInSeconds());

            statsAgent = new StatsAgent(riverConfig);

            int intervalInMs = riverConfig.getStatsdIntervalInSeconds() * 1000;
            timer = new Timer();
            timer.scheduleAtFixedRate(new LogStatsTask(), intervalInMs, intervalInMs);
        }
        else {
            logger.debug("No statsd configuration found. Will not report stats...");
        }

        switch (riverConfig.getActionType()) {
            case INDEX:
                elasticsearchProducer = new IndexDocumentProducer(client, riverConfig, kafkaConsumer, stats);
                break;
            case DELETE:
                elasticsearchProducer = new DeleteDocumentProducer(client, riverConfig, kafkaConsumer, stats);
                break;
            case RAW_EXECUTE:
                elasticsearchProducer = new RawMessageProducer(client, riverConfig, kafkaConsumer, stats);
                break;
        }
    }

    @Override
    public void start() {

        try {
            logger.debug("Index: {}: Starting Kafka River...", riverConfig.getIndexName());
            final KafkaWorker kafkaWorker = new KafkaWorker(kafkaConsumer, elasticsearchProducer, riverConfig, stats);

            thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "Kafka River Worker").newThread(kafkaWorker);
            thread.start();
        } catch (Exception ex) {
            logger.error("Index: {}: Unexpected Error occurred", ex, riverConfig.getIndexName());
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() {
        logger.debug("Index: {}: Closing kafka river...", riverConfig.getIndexName());

        elasticsearchProducer.closeBulkProcessor();
        kafkaConsumer.shutdown();
        
        if(null != timer) {
            timer.cancel();
        }

        thread.interrupt();
    }
    
    private class LogStatsTask extends TimerTask {
        @Override
        public void run() {
            Stats clone = stats.getCloneAndReset();
            logger.debug("Logging stats {}", clone);
            statsAgent.logStats(clone);
        }
    }
}
