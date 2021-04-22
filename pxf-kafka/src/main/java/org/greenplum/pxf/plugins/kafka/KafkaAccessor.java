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

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.plugins.kafka.exception.UnsupportedTypeException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@SuppressWarnings("RedundantThrows")
public class KafkaAccessor extends KafkaBasePlugin implements Accessor {

    private KafkaBatchWriter writer;

    public void setProducer(KafkaProducer<KafkaMessageKey, List<GenericRecord>> producer) {
        kafkaProps.put(KafkaBatchWriter.KAFKA_PRODUCER_CONFIG_KEY, producer);
    }

    @Override
    public boolean openForRead() throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public OneRow readNextObject() throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void closeForRead() throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean openForWrite() throws Exception {

        Schema schema = Schema.createRecord("row", null, null, false);
        schema.setFields(context.getTupleDescription().stream().map(cd -> {
            // in this version of gpdb, external table should not set 'notnull' attribute
            // so we should use union between NULL and another type everywhere
            List<Schema> unionList = new ArrayList<>();
            unionList.add(Schema.create(Schema.Type.NULL));
            DataType type = DataType.get(cd.columnTypeCode());
            Schema targetSchema = Schema.create(map(type));
            getLogicalType(type)
                .ifPresent(logicalType -> logicalType.addToSchema(targetSchema));
            unionList.add(targetSchema);
            Schema fieldSchema = Schema.createUnion(unionList);
            return (new Schema.Field(cd.columnName(), fieldSchema, "", null));
        }).collect(Collectors.toList()));
        context.setMetadata(schema);

        KafkaMessageKey keyTemplate = KafkaMessageKey.template(
            topic, context.getSegmentId(), context.getTotalSegments());
        writer = new KafkaBatchWriter(kafkaProps, schema, topic, keyTemplate, batchSize);

        return true;
    }

    @Override
    public boolean writeNextObject(OneRow onerow) throws Exception {
        return writer.write((GenericRecord) onerow.getData());
    }

    @Override
    public void closeForWrite() throws Exception {
        if (writer != null) {
            writer.close();
        }
    }

    private Schema.Type map(DataType type) {
        switch (type) {
            case BOOLEAN:
                return Schema.Type.BOOLEAN;
            case TEXT:
            case VARCHAR:
                return Schema.Type.STRING;
            case TIMESTAMP:
            case BIGINT:
            case TIME:
                return Schema.Type.LONG;
            case NUMERIC:
            case FLOAT8:
                return Schema.Type.DOUBLE;
            case REAL:
                return Schema.Type.FLOAT;
            case SMALLINT:
            case INTEGER:
            case DATE:
                return Schema.Type.INT;
            default:
                throw new UnsupportedTypeException(type);
        }
    }

    private Optional<LogicalType> getLogicalType(DataType type) {
        switch (type) {
            case TIME:
                return Optional.of(LogicalTypes.timeMicros());
            case TIMESTAMP:
                return Optional.of(LogicalTypes.timestampMicros());
            case DATE:
                return Optional.of(LogicalTypes.date());
            default:
                return Optional.empty();
        }
    }
}
