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
import org.greenplum.pxf.plugins.kafka.codec.type.TimeMicrosLogicalType;

import java.time.LocalTime;

public class TimeMicrosConversion extends Conversion<String> {

    private TimeMicrosConversion() {
        super();
    }

    public static TimeMicrosConversion getInstance() {
        return TimeConversionHolder.INSTANCE;
    }

    @Override
    public Class<String> getConvertedType() {
        return String.class;
    }

    @Override
    public String getLogicalTypeName() {
        return TimeMicrosLogicalType.INSTANCE.getName();
    }

    @Override
    public Schema getRecommendedSchema() {
        return TimeMicrosLogicalType.INSTANCE.addToSchema(Schema.create(Schema.Type.LONG));
    }

    @Override
    public Long toLong(String value, Schema schema, LogicalType type) {
        return LocalTime.parse(value).toNanoOfDay() / 1000;
    }

    private static class TimeConversionHolder {
        private static final TimeMicrosConversion INSTANCE = new TimeMicrosConversion();
    }
}
