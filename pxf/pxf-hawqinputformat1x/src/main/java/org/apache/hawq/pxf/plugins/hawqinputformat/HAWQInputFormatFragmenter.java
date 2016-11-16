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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hawq.pxf.api.Fragment;
import org.apache.hawq.pxf.api.Fragmenter;
import org.apache.hawq.pxf.api.FragmentsStats;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.plugins.hdfs.utilities.HdfsUtilities;

import org.yaml.snakeyaml.Yaml;

import com.pivotal.hawq.mapreduce.ao.file.HAWQAOSplit;
import com.pivotal.hawq.mapreduce.file.HAWQAOFileStatus;
import com.pivotal.hawq.mapreduce.HAWQInputFormat;
import com.pivotal.hawq.mapreduce.metadata.MetadataAccessor;

/**
 * Fragmenter class for HAWQ internal table data resources.
 *
 * Given a HAWQ metadata file divide the data into fragments 
 * and return a list of them along with a list of host:port 
 * locations for each.
 */
public class HAWQInputFormatFragmenter extends Fragmenter {
    private static final Log LOG = LogFactory.getLog(HAWQInputFormatFragmenter.class);

    private Configuration configuration;
    private JobConf jobConf;
    private Job jobContext;

    private HAWQInputFormat fformat;
    private String metadataFile;

    /**
     * Constructs a HAWQInputFormatFragmenter object.
     *
     * @param md all input parameters coming from the client
     * @throws IOException if metadata file could not be retrieved or parsed 
     */
    public HAWQInputFormatFragmenter(InputData md) throws IOException, FileNotFoundException {
        super(md);

        configuration = new Configuration();

        LOG.info("YAML: " + md.getUserProperty("YAML"));

        metadataFile = HdfsUtilities.absoluteDataPath(md.getUserProperty("YAML"));
        HAWQInputFormat.setInput(configuration, metadataFile);

        jobConf = new JobConf(configuration, HAWQInputFormatFragmenter.class);
        jobContext = Job.getInstance(jobConf);

        fformat = new HAWQInputFormat();
    }

    /**
     * Gets the fragments associated with a HAWQ metadata file (URI file name)
     */
    @Override
    public List<Fragment> getFragments() throws Exception {
        List<InputSplit> splits = fformat.getSplits(jobContext);

        for (InputSplit split : splits) {
            String filepath = ((HAWQAOSplit) split).getPath().toUri().getPath();
            String[] hosts = split.getLocations();

            //
            // metadata information includes: file split's start, length and
            // hosts (locations).
            //
            byte[] fragmentMetadata = prepareFragmentMetadata((HAWQAOSplit) split);
            Fragment fragment = new Fragment(filepath, hosts, fragmentMetadata);
            fragments.add(fragment);
        }

        return fragments;
    }

    private static byte[] prepareFragmentMetadata(HAWQAOSplit fsp)
            throws IOException {
        ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
        ObjectOutputStream objectStream = new ObjectOutputStream(
                byteArrayStream);
        objectStream.writeLong(fsp.getStart());
        objectStream.writeLong(fsp.getLength());
        objectStream.writeObject(fsp.getLocations());
        objectStream.writeBoolean(fsp.getChecksum());
        objectStream.writeInt(fsp.getBlockSize());
        objectStream.writeObject(fsp.getCompressType());

        return byteArrayStream.toByteArray();
    }

}
