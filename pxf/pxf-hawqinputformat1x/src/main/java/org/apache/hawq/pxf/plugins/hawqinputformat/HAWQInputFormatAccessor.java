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


import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.ReadAccessor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Plugin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import com.pivotal.hawq.mapreduce.HAWQRecord;
import com.pivotal.hawq.mapreduce.ao.HAWQAOInputFormat;
import com.pivotal.hawq.mapreduce.metadata.HAWQTableFormat;
import com.pivotal.hawq.mapreduce.parquet.HAWQParquetInputFormat;

import java.io.IOException;

/**
 * Accessor for accessing a split-able HAWQ 1.x data sources. 
 */
public abstract class HAWQInputFormatAccessor extends Plugin implements
        ReadAccessor {
    private static final String TABLE_FORMAT = "mapreduce.hawq.table.format";

    protected Configuration conf = null;
    protected RecordReader<Void, HAWQRecord> reader = null;
    protected JobConf jobConf = null;
    protected Job jobContext;

    private HAWQAOInputFormat aoInputFormat = new HAWQAOInputFormat();
    private HAWQParquetInputFormat parquetInputFormat = 
        new HAWQParquetInputFormat();

    /**
     * Constructs an HAWQInputFormatAccessor 
     *
     * @param input all input parameters coming from the client request
     * @param inFormat the HDFS {@link InputFormat} the caller wants to use
     * @throws IOException 
     */
    public HAWQInputFormatAccessor(InputData input) throws IOException {
        super(input);

        // 1. Load Hadoop configuration defined in $HADOOP_HOME/conf/*.xml files
        conf = new Configuration();

        // 2. variable required for the splits iteration logic
        jobConf = new JobConf(conf, HAWQInputFormatAccessor.class);

        jobContext = Job.getInstance(jobConf);
    }

    @Override
    public boolean openForRead() throws Exception {
        /* fopen or similar */
        return true;
    }

    /**
     * Specialized accessors will override this method and implement their own
     * recordReader. For example, a plain delimited text accessor may want to
     * return a LineRecordReader.
     *
     * @param jobConf the hadoop jobconf to use for the selected InputFormat
     * @param split the input split to be read by the accessor
     * @return a recordreader to be used for reading the data records of the
     *         split
     * @throws IOException if recordreader could not be created
     * @throws InterruptedException 
     */
    protected Object getReader(JobConf jobConf, InputSplit split)
            throws IOException, InterruptedException {
        HAWQTableFormat tableFormat = getTableFormat(jobConf);

        switch ( tableFormat ) {
            case AO:
                return aoInputFormat.createRecordReader(split, 
                    new TaskAttemptContextImpl(jobConf, new TaskAttemptID()));
            case Parquet:
                return parquetInputFormat.createRecordReader(split, 
                    new TaskAttemptContextImpl(jobConf, new TaskAttemptID()));
            default:
                throw new IOException("invalid table format: " + tableFormat);
        }
    }

    /**
     * Fetches one record from the file. 
     */
    @Override
    public OneRow readNextObject() throws Exception {
        if (reader.nextKeyValue()) {
            return new OneRow(null, (Object) reader.getCurrentValue());
        }
        else {
            return null;
        }
    }

    /**
     * When user finished reading the file, it closes the RecordReader
     */
    @Override
    public void closeForRead() throws Exception {
        if (reader != null) {
            reader.close();
        }
    }

    private HAWQTableFormat getTableFormat(Configuration conf) {
        String formatName = conf.get(TABLE_FORMAT);
        if (formatName == null) {
            throw new IllegalStateException("Call HAWQInputFormat.setInput");
        }
        return HAWQTableFormat.valueOf(formatName);
    }

}
