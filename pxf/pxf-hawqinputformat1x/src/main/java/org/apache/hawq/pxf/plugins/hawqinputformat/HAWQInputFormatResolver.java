package org.apache.hawq.pxf.plugins.hawqinputformat;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hawq.pxf.api.OneField;
import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.ReadResolver;
import org.apache.hawq.pxf.api.io.DataType;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Plugin;

import com.pivotal.hawq.mapreduce.HAWQRecord;
import com.pivotal.hawq.mapreduce.HAWQException;
import com.pivotal.hawq.mapreduce.schema.HAWQField;
import com.pivotal.hawq.mapreduce.schema.HAWQGroupField;
import com.pivotal.hawq.mapreduce.schema.HAWQPrimitiveField;
import com.pivotal.hawq.mapreduce.schema.HAWQSchema;
import com.pivotal.hawq.mapreduce.schema.HAWQPrimitiveField.PrimitiveType;

/**
 * Class HAWQInputFormatResolver handles deserialization of records that 
 * were serialized in the HAWQ internal table format.
 */
public class HAWQInputFormatResolver extends Plugin implements ReadResolver {

    private static final Log LOG = LogFactory.getLog(HAWQInputFormatResolver.class);

    /**
     * Constructs an HAWQInputFormatResolver. Initializes Avro data structure: the Avro
     * record - fields information and the Avro record reader. All Avro data is
     * build from the Avro schema, which is based on the *.avsc file that was
     * passed by the user
     *
     * @param input all input parameters coming from the client
     * @throws IOException if metadata file could not be retrieved or parsed
     */
    public HAWQInputFormatResolver(InputData input) throws IOException {
        super(input);
    }

    /**
     * Returns a list of the fields of one record. Each record field is
     * represented by a OneField item. OneField item contains two fields: an
     * integer representing the field type and a Java Object representing the
     * field value.
     */
    @Override
    public List<OneField> getFields(OneRow row) throws Exception {

        HAWQRecord hawqRecord = (HAWQRecord) row.getData();
        HAWQSchema schema = hawqRecord.getSchema();
        List<HAWQField> fields = schema.getFields();

        List<OneField> record = new LinkedList<OneField>();

        int currentIndex = 0;

        for (HAWQField field : fields) {
            try {
                currentIndex += populateRecord(record, hawqRecord, field.asPrimitive().getType(), currentIndex + 1);
            } catch (ClassCastException e) {
                throw new Exception("Group fields are not supported yet.");
                // TODO
                //currentIndex += populateRecordGroup(record, field.asGroup(), schema);
            } catch (HAWQException e) {
                throw new Exception(e);
            }
        }
        
        assert (currentIndex == fields.size());

        return record;
    }

    /**
     * For a given field in the HAWQ record we extract its value and insert it
     * into the output {@code List<OneField>} record. A HAWQ field can be a
     * primitive type or an array type.
     *
     * @param record list of fields to be populated
     * @param value HAWQRecord containing the actual data read
     * @param fieldType field data type (enum)
     * @return the number of populated fields
     */
    int populateRecord(List<OneField> record, HAWQRecord value, PrimitiveType fieldType,
                       int index) throws HAWQException {

        int ret = 0;
        String sValue = null;

        switch (fieldType) {
            case BOOL:
                ret = addOneFieldToRecord(record, DataType.BOOLEAN, value.getBoolean(index));
                break;
            case BIT:
                ret = addOneFieldToRecord(record, DataType.BIT, value.getBit(index));
                break;
            case VARBIT:
                ret = addOneFieldToRecord(record, DataType.VARBIT, value.getVarbit(index));
                break;
            case INT2:
                ret = addOneFieldToRecord(record, DataType.SMALLINT, value.getShort(index));
                break;
            case INT4:
                ret = addOneFieldToRecord(record, DataType.INTEGER, value.getInt(index));
                break;
            case INT8:
                ret = addOneFieldToRecord(record, DataType.BIGINT, value.getLong(index));
                break;
            case FLOAT4:
                ret = addOneFieldToRecord(record, DataType.REAL, value.getFloat(index));
                break;
            case FLOAT8:
                ret = addOneFieldToRecord(record, DataType.FLOAT8, value.getDouble(index));
                break;
            case NUMERIC:
                ret = addOneFieldToRecord(record, DataType.NUMERIC, value.getBigDecimal(index));
                break;
            case CHAR:
                ret = addOneFieldToRecord(record, DataType.CHAR, value.getByte(index));
                break;
            // blank padded char is just a TEXT
            case BPCHAR:
                sValue = (value != null) ? String.format("%s", value.getString(index))
                        : null;
                ret = addOneFieldToRecord(record, DataType.BPCHAR, sValue);
                break;
            case VARCHAR:
                sValue = (value != null) ? String.format("%s", value.getString(index))
                        : null;
                ret = addOneFieldToRecord(record, DataType.VARCHAR, sValue);
                break;
            case TEXT:
                sValue = (value != null) ? String.format("%s", value.getString(index))
                        : null;
                ret = addOneFieldToRecord(record, DataType.TEXT, sValue);
                break;
            case DATE:
                ret = addOneFieldToRecord(record, DataType.DATE, value.getDate(index));
                break;
            case TIME:
                ret = addOneFieldToRecord(record, DataType.TIME, value.getTime(index));
                break;
            // TODO implement getTimetz in com.pivotal.hawq.mapreduce.HAWQRecord
            //case TIMETZ:
            //    ret = addOneFieldToRecord(record, DataType.TIMETZ, value);
            //    break;
            case TIMESTAMP:
                ret = addOneFieldToRecord(record, DataType.TIMESTAMP, value.getTimestamp(index));
                break;
            //case TIMESTAMPTZ:
            //    ret = addOneFieldToRecord(record, DataType.TIMESTAMPTZ, value);
            //    break;
            case INTERVAL:
                ret = addOneFieldToRecord(record, DataType.INTERVAL, value.getInterval(index));
                break;
            case POINT:
                ret = addOneFieldToRecord(record, DataType.POINT, value.getPoint(index));
                break;
            case LSEG:
                ret = addOneFieldToRecord(record, DataType.LSEG, value.getLseg(index));
                break;
            case BOX:
                ret = addOneFieldToRecord(record, DataType.BOX, value.getBox(index));
                break;
            case CIRCLE:
                ret = addOneFieldToRecord(record, DataType.CIRCLE, value.getCircle(index));
                break;
            case PATH:
                ret = addOneFieldToRecord(record, DataType.PATH, value.getPath(index));
                break;
            case POLYGON:
                ret = addOneFieldToRecord(record, DataType.POLYGON, value.getPolygon(index));
                break;
            case MACADDR:
                ret = addOneFieldToRecord(record, DataType.MACADDR, value.getMacaddr(index));
                break;
            case INET:
                ret = addOneFieldToRecord(record, DataType.INET, value.getInet(index));
                break;
            case CIDR:
                ret = addOneFieldToRecord(record, DataType.CIDR, value.getCidr(index));
                break;
            case XML:
                sValue = (value != null) ? String.format("%s", value.getString(index))
                        : null;
                ret = addOneFieldToRecord(record, DataType.XML, sValue);
                break;
            default:
                break;
        }
        return ret;
    }

    /**
     * Creates the {@link OneField} object and adds it to the output {@code List<OneField>}
     * record. Binary types are handled accordingly (src/backend/cdb/cdbparquetstoragewrite.c)
     *
     * @param record list of fields to be populated
     * @param gpdbWritableType field type
     * @param val field value
     * @return 1 (number of populated fields)
     */
    int addOneFieldToRecord(List<OneField> record, DataType gpdbWritableType,
                            Object val) {
        OneField oneField = new OneField();
        oneField.type = gpdbWritableType.getOID();
        switch (gpdbWritableType) {
            case BYTEA:
            case BIT:
            case VARBIT:
            case NUMERIC:
            case NAME:
            case CHAR:
            case BPCHAR:
            case VARCHAR:
            case TEXT:
            case XML:
            case TIMETZ:
            case INTERVAL:
            case MACADDR:
            case INET:
            case CIDR:
                if (val instanceof ByteBuffer) {
                    oneField.val = ((ByteBuffer) val).array();
                } else {
                    /**
                     * TODO - not sure what / if anything is necessary here
                     */
                }
                break;
            default:
                oneField.val = val;
                break;
        }

        record.add(oneField);
        return 1;
    }

}
