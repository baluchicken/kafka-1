/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.processor.ProcessorContext;

class ForwardingCacheFlushListener<K, V> implements CacheFlushListener<K, V> {
    private final ProcessorContext context;
    private final boolean sendOldValues;

    ForwardingCacheFlushListener(final ProcessorContext context, final boolean sendOldValues) {
        this.context = context;
        this.sendOldValues = sendOldValues;
    }

    @Override
    public void apply(final K key, final V newValue, final V oldValue) {
        if (sendOldValues) {
            context.forward(key, new Change<>(newValue, oldValue));
        } else {
            context.forward(key, new Change<>(newValue, null));
        }
    }
}
