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
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.Resolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

@SuppressWarnings("RedundantThrows")
public class KafkaResolver extends BasePlugin implements Resolver {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaResolver.class);

    @Override
    public List<OneField> getFields(OneRow row) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public OneRow setFields(List<OneField> record) throws Exception {
        Schema schema = (Schema) context.getMetadata();
        try {
            GenericRecord genericRecord = new GenericData.Record(schema);
            int cnt = 0;
            for (OneField field : record) {
                if (field.type == DataType.BYTEA.getOID()) {
                    // Avro does not seem to understand regular byte arrays
                    field.val = ByteBuffer.wrap((byte[]) field.val);
                } else if (field.type == DataType.SMALLINT.getOID()) {
                    // Avro doesn't have a short, just an int type
                    field.val = (int) (short) field.val;
                }
                genericRecord.put(cnt++, field.val);
            }
            LOG.debug("Incoming record {} converted to generic record {}", record, genericRecord);
            return new OneRow(null, genericRecord);
        } catch (Exception e) {
            String message = String.format("Can't resolve record %s by schema %s", record, schema);
            LOG.error(message, e);
            throw new RuntimeException(message, e);
        }
    }
}
