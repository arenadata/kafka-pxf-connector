/**
 * Copyright © 2021 kafka-pxf-connector
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.greenplum.pxf.plugins.kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public class KafkaBatchWriter implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaBatchWriter.class);

    public static final String KAFKA_PRODUCER_CONFIG_KEY = "kafka.predefined.producer";

    private final KafkaProducer<KafkaMessageKey, List<GenericRecord>> producer;
    private final String topic;
    private final int batchSize;
    private final KafkaMessageKey keyTemplate;
    private int chunk;

    private final List<GenericRecord> buffer;

    private final List<Future<RecordMetadata>> futures = new ArrayList<>();

    public KafkaBatchWriter(Map<String, Object> kafkaProps, Schema schema, String topic, KafkaMessageKey keyTemplate, int batchSize) {
        @SuppressWarnings("unchecked") KafkaProducer<KafkaMessageKey, List<GenericRecord>> producer =
                (KafkaProducer<KafkaMessageKey, List<GenericRecord>>) kafkaProps.get(KAFKA_PRODUCER_CONFIG_KEY);
        this.producer = producer != null ? producer : new KafkaProducer<>(kafkaProps,
                new AvroReflectBeanSerializer<>(KafkaMessageKey.SCHEMA),
                new AvroGenericCollectionSerializer(schema));
        this.topic = topic;
        this.keyTemplate = keyTemplate;
        this.batchSize = batchSize;
        this.buffer = new ArrayList<>(batchSize);
        this.chunk = 0;
    }

    public boolean write(GenericRecord record) {
        if (buffer.size() >= batchSize) {
            writeBuffer(false);
        }
        boolean r = buffer.add(record);
        LOG.debug("Record {} was added to buffer (size={}, result={})", record, buffer.size(), r);
        return r;
    }

    @Override
    @SuppressWarnings("RedundantThrows")
    public void close() throws IOException {
        try {
            writeBuffer(true);
            for (Future<RecordMetadata> future : futures) {
                if (future != null) {
                    @SuppressWarnings("unused")
                    RecordMetadata metadata = future.get();
                    if (metadata == null) {
                        LOG.warn("Unexpected null metadata while closing writer!");
                    } else {
                        LOG.debug("Kafka producer finished with metadata {}", metadata);
                    }
                }
            }
        } catch (Exception e) {
            String message = String.format("Can't finish sending data to topic '%s' from segment %d/%d",
                    topic, keyTemplate.getStreamNumber(), keyTemplate.getStreamTotal());
            LOG.error(message, e);
            throw new RuntimeException(message, e);
        } finally {
            producer.flush();
            producer.close();
        }
    }

    private void writeBuffer(boolean last) {
        LOG.debug("Sending to kafka topic '{}' {} records from buffer (last={})", topic, buffer.size(), last);
        if (buffer.size() > 0 || last) {
            KafkaMessageKey messageKey = keyTemplate.withChunkInfo(chunk++, last);
            futures.add(producer.send(new ProducerRecord<>(topic, messageKey, buffer)));
            buffer.clear();
        }
    }
}
