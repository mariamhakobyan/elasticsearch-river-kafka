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

import java.util.Set;

/**
 * An interface to abstract operations done from kafka messages
 */
public interface Producer {

    public void addMessagesToBulkProcessor(final Set<MessageAndMetadata> messageSet);

    public void closeBulkProcessor();
}
