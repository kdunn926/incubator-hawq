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


import org.apache.hawq.pxf.api.OneField;
import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.ReadResolver;
import org.apache.hawq.pxf.api.io.DataType;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Plugin;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import com.pivotal.hawq.mapreduce.HAWQRecord;
import com.pivotal.hawq.mapreduce.schema.HAWQField;
import com.pivotal.hawq.mapreduce.schema.HAWQGroupField;
import com.pivotal.hawq.mapreduce.schema.HAWQSchema;

/**
 * Class HAWQInputFormatResolver handles deserialization of records that 
 * were serialized in the HAWQ internal table format.
 */
public class HAWQInputFormatResolver extends Plugin implements ReadResolver {

    /**
     * Constructs an HAWQInputFormatResolver. Initializes Avro data structure: the Avro
     * record - fields information and the Avro record reader. All Avro data is
     * build from the Avro schema, which is based on the *.avsc file that was
     * passed by the user
     *
     * @param input all input parameters coming from the client
     * @throws IOException if Avro schema could not be retrieved or parsed
     */
    public HAWQInputFormatResolver(InputData input) throws IOException {
        super(input);

        //fformat = new HAWQInputFormat();

        //String metadataFile = HdfsUtilities.absoluteDataPath(md.getDataSource());

        //HAWQInputFormat.setInput(configuration, metadataFile);
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
            currentIndex += populateRecord(record, field.asGroup(), schema);
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
     * @param fieldValue field value
     * @param fieldSchema field schema
     * @return the number of populated fields
     */
    int populateRecord(List<OneField> record, HAWQGroupField field,
                       HAWQSchema fieldSchema) {

        String fieldType = field.getDataTypeName();
        int ret = 0;
        Object value = field.asPrimitive();
        //boolean isArray = field.isArray();
        String sValue = null;

        switch (fieldType) {
            case "bool":
                ret = addOneFieldToRecord(record, DataType.BOOLEAN, value);
                break;
            case "bit":
                ret = addOneFieldToRecord(record, DataType.ZPBIT, value);
                break;
            case "varbit":
                ret = addOneFieldToRecord(record, DataType.VARBIT, value);
                break;
            case "int2":
                ret = addOneFieldToRecord(record, DataType.SMALLINT, value);
                break;
            case "int4":
                ret = addOneFieldToRecord(record, DataType.INTEGER, value);
                break;
            case "int8":
                ret = addOneFieldToRecord(record, DataType.BIGINT, value);
                break;
            case "float4":
                ret = addOneFieldToRecord(record, DataType.REAL, value);
                break;
            case "float8":
                ret = addOneFieldToRecord(record, DataType.FLOAT8, value);
                break;
            case "numeric":
                ret = addOneFieldToRecord(record, DataType.NUMERIC, value);
                break;
            case "char":
                ret = addOneFieldToRecord(record, DataType.CHAR, value);
                break;
            case "bpchar":
                ret = addOneFieldToRecord(record, DataType.BPCHAR, value);
                break;
            case "varchar":
            	sValue = (value != null) ? String.format("%s", value)
                        : null;
                ret = addOneFieldToRecord(record, DataType.VARCHAR, sValue);
                break;
            case "text":
            	sValue = (value != null) ? String.format("%s", value)
                        : null;
                ret = addOneFieldToRecord(record, DataType.TEXT, sValue);
                break;
            case "date":
                ret = addOneFieldToRecord(record, DataType.DATE, value);
                break;
            case "time":
                ret = addOneFieldToRecord(record, DataType.TIME, value);
                break;
            case "timez":
                ret = addOneFieldToRecord(record, DataType.TIMETZ, value);
                break;
            case "timestamp":
                ret = addOneFieldToRecord(record, DataType.TIMESTAMP, value);
                break;
            case "timestampz":
                ret = addOneFieldToRecord(record, DataType.TIMESTAMPTZ, value);
                break;
            case "interval":
                ret = addOneFieldToRecord(record, DataType.INTERVAL, value);
                break;
            case "point":
                ret = addOneFieldToRecord(record, DataType.POINT, value);
                break;
            case "lseg":
                ret = addOneFieldToRecord(record, DataType.LSEG, value);
                break;
            case "box":
                ret = addOneFieldToRecord(record, DataType.BOX, value);
                break;
            case "circle":
                ret = addOneFieldToRecord(record, DataType.CIRCLE, value);
                break;
            case "path":
                ret = addOneFieldToRecord(record, DataType.PATH, value);
                break;
            case "polygon":
                ret = addOneFieldToRecord(record, DataType.POLYGON, value);
                break;
            /*
            case "macaddr":
                ret = addOneFieldToRecord(record, DataType.MACADDR, value);
                break;
            */
            case "inet":
                ret = addOneFieldToRecord(record, DataType.INET, value);
                break;
            case "cidr":
                ret = addOneFieldToRecord(record, DataType.CIDR, value);
                break;
            /*
            case "xml":
                ret = addOneFieldToRecord(record, DataType.XML, value);
                break;
            */
            default:
                break;
        }
        return ret;
    }

    /**
     * Creates the {@link OneField} object and adds it to the output {@code List<OneField>}
     * record. 
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
