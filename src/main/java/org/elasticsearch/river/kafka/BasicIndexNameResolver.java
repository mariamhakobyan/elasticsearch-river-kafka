package org.elasticsearch.river.kafka;

/**
 * Created by kangell on 4/14/2015.
 */
public class BasicIndexNameResolver implements IndexNameResolver {
    private final RiverConfig config;

    public BasicIndexNameResolver(RiverConfig config) {
        this.config = config;
    }

    @Override
    public String getIndexName() {
        return config.getIndexName();
    }
}
