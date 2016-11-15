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
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hawq.pxf.plugins.hdfs.HdfsSplittableDataAccessor;

import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.ReadAccessor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Plugin;
import org.apache.hawq.pxf.plugins.hdfs.utilities.HdfsUtilities;

import com.pivotal.hawq.mapreduce.HAWQRecord;
import com.pivotal.hawq.mapreduce.HAWQInputFormat;
import com.pivotal.hawq.mapreduce.ao.HAWQAOInputFormat;
import com.pivotal.hawq.mapreduce.ao.HAWQAORecordReader;
import com.pivotal.hawq.mapreduce.metadata.HAWQTableFormat;
import com.pivotal.hawq.mapreduce.metadata.HAWQAOTableMetadata;
import com.pivotal.hawq.mapreduce.metadata.MetadataAccessor;

/**
 * Accessor for accessing a split-able HAWQ 1.x data sources. 
 */
public class HAWQInputFormatAccessor extends HdfsSplittableDataAccessor {
    private static final Log LOG = LogFactory.getLog(HAWQInputFormatAccessor.class);

    private static final String TABLE_FORMAT = "mapreduce.hawq.table.format";

    private Configuration configuration;
    private JobConf jobConf;
    private Job jobContext;

    //protected RecordReader<Void, HAWQRecord> reader = null;

    private HAWQAOInputFormat aoInputFormat = null; //new HAWQAOInputFormat();

    /**
     * Constructs an HAWQInputFormatAccessor 
     *
     * @param input all input parameters coming from the client request
     * @throws IOException if Hadoop XML files could not be retrieved or parsed 
     */
    public HAWQInputFormatAccessor(InputData input) throws Exception {
        super(input, null);
        LOG.info("Enter Accessor()");

        aoInputFormat = new HAWQAOInputFormat();

        // 1. Load Hadoop configuration defined in $HADOOP_HOME/conf/*.xml files
        configuration = new Configuration();

        String metadataFile = HdfsUtilities.absoluteDataPath(input.getUserProperty("YAML"));
        LOG.info("YAML: " + metadataFile);

        HAWQAOTableMetadata tableMetadata = MetadataAccessor.newInstanceUsingFile(metadataFile).getAOMetadata();
        HAWQAOInputFormat.setInput(configuration, tableMetadata);

        jobConf = new JobConf(configuration, HAWQInputFormatFragmenter.class);
        jobContext = Job.getInstance(jobConf);

        LOG.info("Instantiated Accessor");

    }

    /*
    @Override
    public boolean openForRead() throws Exception {
        // fopen or similar 
        return true;
    }
    */

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
     */

    @Override
    protected Object getReader(JobConf jobConf, InputSplit split)
            throws IOException {
        try {
            HAWQAORecordReader reader = new HAWQAORecordReader();
            reader.initialize((FileSplit) split,
                new TaskAttemptContextImpl(jobConf, new TaskAttemptID()));
            return (RecordReader) reader; //new HAWQAORecordReader();
            //return aoInputFormat.createRecordReader((FileSplit) split, 
            //    new TaskAttemptContextImpl(conf, new TaskAttemptID()));
        } catch (InterruptedException e) {
            return null;
            //throw new IOException("Unable to instantiate AOInputFormat RecordReader: " + e);
        }
        
    }

    /**
     * Fetches one record from the file. 
     */
    /*
    @Override
    public OneRow readNextObject() throws Exception {
        if (reader.nextKeyValue()) {
            return new OneRow(null, (Object) reader.getCurrentValue());
        }
        else {
            return null;
        }
    }
    */

    /**
     * When user finished reading the file, it closes the RecordReader
     */
    /*
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

    @Override
    public boolean isThreadSafe() {
        return false;
        //return HdfsUtilities.isThreadSafe(inputData.getDataSource(),
        //inputData.getUserProperty("COMPRESSION_CODEC"));
    }
    */
}
