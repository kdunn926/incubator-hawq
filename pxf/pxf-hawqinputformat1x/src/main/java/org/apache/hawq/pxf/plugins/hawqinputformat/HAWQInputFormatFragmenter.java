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


import org.apache.hawq.pxf.api.Fragment;
import org.apache.hawq.pxf.api.Fragmenter;
import org.apache.hawq.pxf.api.FragmentsStats;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.plugins.hdfs.utilities.HdfsUtilities;
import com.pivotal.hawq.mapreduce.HAWQInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.List;

/**
 * Fragmenter class for HAWQ internal table data resources.
 *
 * Given a HAWQ metadata file divide the data into fragments 
 * and return a list of them along with a list of host:port 
 * locations for each.
 */
public class HAWQInputFormatFragmenter extends Fragmenter {
    private Configuration configuration;
    private JobConf jobConf;
    private Job jobContext;

    private HAWQInputFormat fformat;

    /**
     * Constructs an HdfsDataFragmenter object.
     *
     * @param md all input parameters coming from the client
     * @throws IOException if metadata file could not be retrieved or parsed 
     */
    public HAWQInputFormatFragmenter(InputData md) throws IOException {
        super(md);

        configuration = new Configuration();

        jobConf = new JobConf(configuration, HAWQInputFormatFragmenter.class);
        jobContext = Job.getInstance(jobConf);

        fformat = new HAWQInputFormat();

        String metadataFile = HdfsUtilities.absoluteDataPath(md.getDataSource());

        HAWQInputFormat.setInput(configuration, metadataFile);
    }

    /**
     * Gets the fragments associated with a HAWQ metadata file (URI file name)
     * Returns the data fragments in JSON format.
     */
    @Override
    public List<Fragment> getFragments() throws Exception {
        //String absoluteDataPath = HdfsUtilities.absoluteDataPath(inputData.getDataSource());
        List<InputSplit> splits = fformat.getSplits(jobContext);

        for (InputSplit split : splits) {
            FileSplit fsp = (FileSplit) split;

            String filepath = fsp.getPath().toUri().getPath();
            String[] hosts = fsp.getLocations();

            /*
             * metadata information includes: file split's start, length and
             * hosts (locations).
             */
            byte[] fragmentMetadata = HdfsUtilities.prepareFragmentMetadata(fsp);
            Fragment fragment = new Fragment(filepath, hosts, fragmentMetadata);
            fragments.add(fragment);
        }

        return fragments;
    }

    @Override
    public FragmentsStats getFragmentsStats() throws Exception {
        List<InputSplit> splits = fformat.getSplits(jobContext);

        if (splits.isEmpty()) {
            return new FragmentsStats(0, 0, 0);
        }
        long totalSize = 0;
        for (InputSplit split: splits) {
            totalSize += split.getLength();
        }
        InputSplit firstSplit = splits.get(0);
        return new FragmentsStats(splits.size(), firstSplit.getLength(), totalSize);
    }

}
