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
import org.apache.kafka.common.serialization.Serializer;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class KafkaAccessorTest {

    public static final int BATCH_SIZE = 2;
    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();
    RequestContext context;
    @Spy
    private KafkaAccessor accessor;
    private KafkaResolver resolver;
    @Mock
    private KafkaProducer<KafkaMessageKey, List<GenericRecord>> producer;

    @Before
    public void setup() {
        context = new RequestContext();
        context.setSegmentId(0);
        context.setTotalSegments(1);
        context.setUser("user");
        context.setConfig("default");
        context.setDataSource("test");
        context.setTupleDescription(Arrays.asList(
            new ColumnDescriptor("id", DataType.BIGINT.getOID(), 0, null, null),
            new ColumnDescriptor("timestamp", DataType.TIMESTAMP.getOID(), 0, null, null),
            new ColumnDescriptor("time", DataType.TIME.getOID(), 0, null, null),
            new ColumnDescriptor("date", DataType.DATE.getOID(), 0, null, null),
            new ColumnDescriptor("name", DataType.VARCHAR.getOID(), 0, null, null)));
        context.setAdditionalConfigProps(new HashMap<String, String>() {{
            put("kafka.bootstrap.servers", "localhost");
            put("kafka.batch.size", Integer.toString(BATCH_SIZE));
        }});
        doReturn(true).when(accessor).topicExists(anyString(), anyString());
        accessor.initialize(context);
        accessor.setProducer(producer);
        resolver = new KafkaResolver();
        resolver.initialize(context);
        when(producer.send(any())).thenAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ProducerRecord<KafkaMessageKey, List<GenericRecord>> record =
                (ProducerRecord<KafkaMessageKey, List<GenericRecord>>) invocation.getArguments()[0];
            Serializer<KafkaMessageKey> keySerializer = new AvroReflectBeanSerializer<>(KafkaMessageKey.SCHEMA);
            Assert.assertNotNull(keySerializer.serialize(record.topic(), record.key()));
            AvroGenericCollectionSerializer serializer = new AvroGenericCollectionSerializer((Schema) context.getMetadata());
            Assert.assertNotNull(serializer.serialize(record.topic(), record.value()));
            return null;
        });
    }

    @Test
    public void testSuccessfulWrite() throws Exception {
        mainCycleWithChecks(Arrays.asList(
            Arrays.asList(
                new OneField(DataType.BIGINT.getOID(), 1L),
                new OneField(DataType.TIMESTAMP.getOID(), "2020-10-23 10:00:00"),
                new OneField(DataType.TIME.getOID(), "10:00:00"),
                new OneField(DataType.DATE.getOID(), "2020-10-23"),
                new OneField(DataType.VARCHAR.getOID(), "a"))
        ));
    }

    @Test
    @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
    public void testBadFormatMoreFields() throws Exception {
        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage("Can't resolve record");
        mainCycleWithChecks(Arrays.asList(
            Arrays.asList(
                new OneField(DataType.BIGINT.getOID(), 1),
                new OneField(DataType.VARCHAR.getOID(), "a"),
                new OneField(DataType.VARCHAR.getOID(), "b"),
                new OneField(DataType.VARCHAR.getOID(), "c"),
                new OneField(DataType.VARCHAR.getOID(), "x"),
                new OneField(DataType.VARCHAR.getOID(), "d"))
        ));
    }

    @Test
    @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
    public void testBadFormatLessFields() throws Exception {
        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage("Can't finish sending data to topic 'test'");
        mainCycleWithChecks(Arrays.asList(
            Arrays.asList(
                new OneField(DataType.VARCHAR.getOID(), "b"))
        ));
    }

    @Test
    @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
    public void testBadFormat() throws Exception {
        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage("Can't finish sending data to topic 'test'");
        mainCycleWithChecks(Arrays.asList(
            Arrays.asList(
                new OneField(DataType.VARCHAR.getOID(), "a"),
                new OneField(DataType.VARCHAR.getOID(), "b"))
        ));
    }

    private void mainCycleWithChecks(List<List<OneField>> data) throws Exception {
        Assert.assertTrue(accessor.openForWrite());
        for (List<OneField> record : data) {
            OneRow row = resolver.setFields(record);
            Assert.assertTrue(accessor.writeNextObject(row));
        }
        accessor.closeForWrite();
        verify(producer, times(data.size() / BATCH_SIZE + 1)).send(any());
        verify(producer, atLeastOnce()).flush();
        verify(producer, atLeastOnce()).close();
    }
}
