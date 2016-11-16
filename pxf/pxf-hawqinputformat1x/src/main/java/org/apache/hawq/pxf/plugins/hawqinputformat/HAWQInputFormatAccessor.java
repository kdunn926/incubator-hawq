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
import java.lang.InterruptedException;
import java.util.LinkedList;
import java.util.ListIterator;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.ReadAccessor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Plugin;
import org.apache.hawq.pxf.plugins.hdfs.utilities.HdfsUtilities;

import com.pivotal.hawq.mapreduce.HAWQRecord;
import com.pivotal.hawq.mapreduce.HAWQInputFormat;
import com.pivotal.hawq.mapreduce.ao.HAWQAOInputFormat;
import com.pivotal.hawq.mapreduce.ao.HAWQAORecordReader;
import com.pivotal.hawq.mapreduce.ao.file.HAWQAOSplit;
import com.pivotal.hawq.mapreduce.metadata.HAWQTableFormat;
import com.pivotal.hawq.mapreduce.metadata.HAWQAOTableMetadata;
import com.pivotal.hawq.mapreduce.metadata.MetadataAccessor;

/**
 * Accessor for accessing a split-able HAWQ 1.x data sources. 
 */
public class HAWQInputFormatAccessor extends Plugin implements
        ReadAccessor {
    private static final Log LOG = LogFactory.getLog(HAWQInputFormatAccessor.class);

    private Configuration conf;
    private Job jobContext;
    private TaskAttemptContext taskContext;

    protected RecordReader<Void, HAWQRecord> reader = null;
    protected ListIterator<HAWQAOSplit> iter = null;
    protected Object rowData;

    /**
     * Constructs an HAWQInputFormatAccessor 
     *
     * @param input all input parameters coming from the client request
     * @throws IOException if Hadoop XML files could not be retrieved or parsed 
     */
    public HAWQInputFormatAccessor(InputData input) throws Exception {
        super(input);

        // Load Hadoop configuration defined in $HADOOP_HOME/conf/*.xml files
        conf = new Configuration();

        String metadataFile = HdfsUtilities.absoluteDataPath(input.getUserProperty("YAML"));
        LOG.info("YAML: " + metadataFile);

        HAWQAOTableMetadata tableMetadata = MetadataAccessor.newInstanceUsingFile(metadataFile).getAOMetadata();
        HAWQAOInputFormat.setInput(conf, tableMetadata);

        jobContext = Job.getInstance(conf);

        taskContext = new TaskAttemptContextImpl(conf, new TaskAttemptID());

        reader = new HAWQAORecordReader();
    }

    /**
     * Fetches the requested fragment (HAWQAOSplit) for the current client
     * request, and sets a record reader for the job.
     *
     * @return true if succeeded, false if no more splits to be read
     */
    @Override
    public boolean openForRead() throws Exception {
        LinkedList<HAWQAOSplit> requestSplits = new LinkedList<HAWQAOSplit>();

        HAWQAOSplit fileSplit = parseFragmentMetadata(inputData);
        requestSplits.add(fileSplit);

        // Initialize record reader based on current split
        iter = requestSplits.listIterator(0);
        return getNextSplit();
    }

    /**
     * Sets the current split and initializes a RecordReader who feeds from the
     * split
     *
     * @return true if there is a split to read
     * @throws IOException if record reader could not be created
     */
    @SuppressWarnings(value = "unchecked")
    protected boolean getNextSplit() throws Exception  {
        if (!iter.hasNext()) {
            return false;
        }

        HAWQAOSplit currSplit = iter.next();

        reader.initialize(currSplit, taskContext); 
        rowData = reader.getCurrentValue(); 
        return true;
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

    @Override
    public boolean isThreadSafe() {
        return false;
    }

    /**
     * Parses fragment metadata and return matching {@link HAWQAOSplit}.
     *
     * @param inputData request input data
     * @return HAWAOSplit with fragment metadata
     */
    public static HAWQAOSplit parseFragmentMetadata(InputData inData) {
        try {
            byte[] serializedLocation = inData.getFragmentMetadata();
            if (serializedLocation == null) {
                throw new IllegalArgumentException(
                        "Missing fragment location information");
            }

            ByteArrayInputStream bytesStream = new ByteArrayInputStream(
                    serializedLocation);
            ObjectInputStream objectStream = new ObjectInputStream(bytesStream);

            long start = objectStream.readLong();
            long end = objectStream.readLong();

            String[] hosts = (String[]) objectStream.readObject();

            boolean checksum = objectStream.readBoolean();
            int blockSize = objectStream.readInt(); 
            String compressType = (String) objectStream.readObject();

            HAWQAOSplit fileSplit = new HAWQAOSplit(new Path(
                    inData.getDataSource()), start, end, hosts,
                    checksum, compressType, blockSize);

            LOG.debug("parsed file split: path " + inData.getDataSource()
                    + ", start " + start + ", end " + end + ", hosts "
                    + ArrayUtils.toString(hosts) + ", checksum " + checksum
                    + ", compressType " + compressType + ", blockSize " + blockSize);

            return fileSplit;

        } catch (Exception e) {
            throw new RuntimeException(
                    "Exception while reading expected fragment metadata", e);
        }
    }
}
