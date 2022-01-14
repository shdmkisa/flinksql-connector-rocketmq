/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.flink.source.util;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.data.util.DataFormatConverters.TimestampConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Set;

/** String serializer. */
public class StringSerializer {

    public static TimestampConverter timestampConverter = new TimestampConverter(3);
    private static final Base64.Decoder DECODER = Base64.getDecoder();
    //private static SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
   // private static SimpleDateFormat sdf2= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static DateTimeFormatter sdf1=DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
    private static DateTimeFormatter sdf2=DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final Logger LOGGER =
            LoggerFactory.getLogger(StringSerializer.class);
    public static Object deserialize(
            String value,
            ByteSerializer.ValueType type,
            DataType dataType,
            Set<String> nullValues) {
        return deserialize(value, type, dataType, nullValues, false);
    }

    public static Object deserialize(
            String value,
            ByteSerializer.ValueType type,
            DataType dataType,
            Set<String> nullValues,
            Boolean isRGData) {
        if (null != nullValues && nullValues.contains(value)) {
            return null;
        }
        switch (type) {
            case V_ByteArray: // byte[]
                if (isRGData) {
                    byte[] bytes = null;
                    try {
                        bytes = DECODER.decode(value);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return bytes;
                } else {
                    return value.getBytes();
                }
            case V_String:
                return BinaryStringData.fromString(value);
            case V_Byte: // byte
                return null == value ? null : Byte.parseByte(value);
            case V_Short:
                return null == value ? null : Short.parseShort(value);
            case V_Integer:
                return null == value ? null : Integer.parseInt(value);
            case V_Long:
                return null == value ? null : Long.parseLong(value);
            case V_Float:
                return null == value ? null : Float.parseFloat(value);
            case V_Double:
                return null == value ? null : Double.parseDouble(value);
            case V_Boolean:
                return null == value ? null : parseBoolean(value);
            case V_Timestamp: // sql.Timestamp encoded as long
                if (isRGData) {
                    return null == value ? null : Long.parseLong(value);
                }
                if (null == value) {
                    return null;
                } else {
                    try {
                        return timestampConverter.toInternal(new Timestamp(Long.parseLong(value)));
                    } catch (NumberFormatException e) {
                        return timestampConverter.toInternal(Timestamp.valueOf(value));
                    }
                }
            case V_Date:
            case V_LocalDate:// sql.Date encoded as long
                if (isRGData) {
                    return null == value ? null : Long.parseLong(value);
                }
                return null == value
                        ? null
                        : DataFormatConverters.DateConverter.INSTANCE.toInternal(
                                Date.valueOf(value));
            case V_Time:
            case V_LocalTime:// sql.Time encoded as long
                if (isRGData) {
                    return null == value ? null : Long.parseLong(value);
                }
                try {
                    return null == value ? null : DataFormatConverters.TimeConverter.INSTANCE.toInternal(new Time(Long.parseLong(value)));
                }catch (NumberFormatException e){
                    return null == value ? null : DataFormatConverters.TimeConverter.INSTANCE.toInternal(Time.valueOf(value));
                }
            case V_BigDecimal:
                DecimalType decimalType = (DecimalType) dataType.getLogicalType();
                return value == null
                        ? null
                        : DecimalData.fromBigDecimal(
                                new BigDecimal(value),
                                decimalType.getPrecision(),
                                decimalType.getScale());
            case V_BigInteger:
                return null == value ? null : new BigInteger(value);

            case V_LocalDateTime:
                try {
                    DateTimeFormatter sdf=sdf2;
                    if(value.contains("T")) sdf =sdf1;
                    LocalDateTime localDateTime = LocalDateTime.parse(value, sdf);
                    Timestamp timestamp = new Timestamp(localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli());
                    return null == value ? null : timestampConverter.toInternal(timestamp);
                }catch (Exception e){
                    e.printStackTrace();
                }

            default:
                throw new IllegalArgumentException();
        }
    }

    public static Object deserialize(
            String value, ByteSerializer.ValueType type, DataType dataType, Boolean isRGData) {
        return deserialize(value, type, dataType, null, isRGData);
    }

    public static Boolean parseBoolean(String s) {
        if (s != null) {
            if (s.equalsIgnoreCase("true") || s.equalsIgnoreCase("false")) {
                return Boolean.valueOf(s);
            }

            if (s.equals("1")) {
                return Boolean.TRUE;
            }

            if (s.equals("0")) {
                return Boolean.FALSE;
            }
        }

        throw new IllegalArgumentException();
    }
}
