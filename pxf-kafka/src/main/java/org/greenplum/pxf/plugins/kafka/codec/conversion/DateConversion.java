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
package org.greenplum.pxf.plugins.kafka.codec.conversion;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.greenplum.pxf.plugins.kafka.codec.type.DateLogicalType;

import java.sql.Date;
import java.time.LocalDate;

public class DateConversion extends Conversion<String> {

    private DateConversion() {
        super();
    }

    public static DateConversion getInstance() {
        return DateConversionHolder.INSTANCE;
    }

    @Override
    public Class<String> getConvertedType() {
        return String.class;
    }

    @Override
    public String getLogicalTypeName() {
        return DateLogicalType.INSTANCE.getName();
    }

    @Override
    public Schema getRecommendedSchema() {
        return DateLogicalType.INSTANCE.addToSchema(Schema.create(Schema.Type.INT));
    }

    @Override
    public Integer toInt(String value, Schema schema, LogicalType type) {
        return Long.valueOf(LocalDate.parse(value).toEpochDay()).intValue();
    }

    private static class DateConversionHolder {
        private static final DateConversion INSTANCE = new DateConversion();
    }
}
