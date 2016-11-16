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


// PXF uses these
//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapred.FileSplit;
//import org.apache.hadoop.mapred.InputSplit;

// hawq-hadoop uses these
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
//import org.apache.hadoop.mapreduce.JobConf;
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

    //private static final String TABLE_FORMAT = "mapreduce.hawq.table.format";

    private Configuration conf;
    //private JobConf jobConf;
    private Job jobContext;
    private TaskAttemptContext taskContext;

    protected RecordReader<Void, HAWQRecord> reader = null;
    protected ListIterator<HAWQAOSplit> iter = null;
    protected Object rowData;

    //private HAWQAOInputFormat aoInputFormat = null; //new HAWQAOInputFormat();

    /**
     * Constructs an HAWQInputFormatAccessor 
     *
     * @param input all input parameters coming from the client request
     * @throws IOException if Hadoop XML files could not be retrieved or parsed 
     */
    public HAWQInputFormatAccessor(InputData input) throws Exception {
        super(input);
        LOG.info("Enter Accessor()");

        //aoInputFormat = new HAWQAOInputFormat();

        // 1. Load Hadoop configuration defined in $HADOOP_HOME/conf/*.xml files
        conf = new Configuration();

        String metadataFile = HdfsUtilities.absoluteDataPath(input.getUserProperty("YAML"));
        LOG.info("YAML: " + metadataFile);

        HAWQAOTableMetadata tableMetadata = MetadataAccessor.newInstanceUsingFile(metadataFile).getAOMetadata();
        HAWQAOInputFormat.setInput(conf, tableMetadata);

        //jobConf = new JobConf(conf, HAWQInputFormatFragmenter.class);
        jobContext = Job.getInstance(conf);

        taskContext = new TaskAttemptContextImpl(conf, new TaskAttemptID());

        reader = new HAWQAORecordReader();

        LOG.info("Instantiated Accessor");

    }

    /**
     * Fetches the requested fragment (file split) for the current client
     * request, and sets a record reader for the job.
     *
     * @return true if succeeded, false if no more splits to be read
     */
    @Override
    public boolean openForRead() throws Exception {
        LinkedList<HAWQAOSplit> requestSplits = new LinkedList<HAWQAOSplit>();

        //org.apache.hadoop.mapred.FileSplit castedInput =
        //    (org.apache.hadoop.mapred.FileSplit) inputData; 
        // Round-peg square-hole the v1 MR API to v2
        //FileSplit convertedSplit = 
        //    new FileSplit(castedInput.getPath(), castedInput.getStart(), 
        //                  castedInput.getLength(), castedInput.getLocations());

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

        // Round-peg square-hole the v1 MR API to v2
        FileSplit convertedCurrSplit = 
            new FileSplit(currSplit.getPath(), currSplit.getStart(), 
                          currSplit.getLength(), currSplit.getLocations());

        reader.initialize(currSplit, taskContext); 
        rowData = reader.getCurrentValue(); //createValue();
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
     */

    /*
    @Override
    protected Object getReader(JobConf jobConf, InputSplit split)
            throws IOException {
        return aoInputFormat.createRecordReader((FileSplit) split,
                new TaskAttemptContextImpl(conf, new TaskAttemptID()));;
        //} catch (InterruptedException e) {
        //    throw new IOException("Unable to instantiate AOInputFormat RecordReader: " + e);
        //}
        
    }
    */

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

    /*
    private HAWQTableFormat getTableFormat(Configuration conf) {
        String formatName = conf.get(TABLE_FORMAT);
        if (formatName == null) {
            throw new IllegalStateException("Call HAWQInputFormat.setInput");
        }
        return HAWQTableFormat.valueOf(formatName);
    }
    */

    @Override
    public boolean isThreadSafe() {
        return false;
        //return HdfsUtilities.isThreadSafe(inputData.getDataSource(),
        //inputData.getUserProperty("COMPRESSION_CODEC"));
    }

    /**
     * Parses fragment metadata and return matching {@link FileSplit}.
     *
     * @param inputData request input data
     * @return FileSplit with fragment metadata
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
