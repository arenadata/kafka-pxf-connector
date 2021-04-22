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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.greenplum.pxf.plugins.kafka.utils.AvroData;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaTest {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTest.class);

    public static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9094";
    public static final String KAFKA_TOPIC = "demo";

    @Test
    @Ignore
    public void testProducer() throws ExecutionException, InterruptedException {
        KafkaProducer<String, String> producer = new KafkaProducer<>(
                Collections.singletonMap(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS),
                new StringSerializer(), new StringSerializer());
        ProducerRecord<String, String> record = new ProducerRecord<>(KAFKA_TOPIC, "qwe", "123");
        Future<RecordMetadata> future = producer.send(record);
        RecordMetadata metadata = future.get();
        Assert.assertNotNull(metadata);
        producer.close();
    }

    @Test
    @Ignore
    public void testConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, Long.toHexString(System.currentTimeMillis()));
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties,
                new StringDeserializer(), new StringDeserializer());
        consumer.subscribe(Collections.singletonList(KAFKA_TOPIC));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
            if (records.isEmpty()) {
                break;
            } else {
                records.forEach(r -> {
                    System.out.println(r.key());
                    System.out.println(r.value());
                });
                consumer.commitAsync();
            }
        }
        consumer.close();
    }

    @Test
    @Ignore
    public void testAvroConsumer() throws IOException {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, Long.toHexString(System.currentTimeMillis()));
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties,
                new ByteArrayDeserializer(), new ByteArrayDeserializer());
        consumer.subscribe(Collections.singletonList(KAFKA_TOPIC));
        while (true) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(2000));
            if (records.isEmpty()) {
                break;
            } else {
                for (ConsumerRecord<byte[], byte[]> r : records) {
                    LOG.info(AvroData.convert(r.key()).toString());
                    LOG.info(AvroData.convert(r.value()).toString());
                }
                consumer.commitAsync();
            }
        }
        consumer.close();
    }
}
