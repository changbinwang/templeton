/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hcatalog.templeton;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.exec.ExecuteException;
import org.apache.hcatalog.templeton.tool.TempletonUtils;
import org.apache.hcatalog.templeton.tool.TempletonStreamJob;

/**
 * Submit a streaming job to the MapReduce queue.  We do this by
 * running the hadoop executable on the local server using the
 * ExecService.  This allows us to easily verify that the user
 * identity is being securely used.
 *
 * This is the backend of the mapreduce/streaming web service.
 */
public class StreamingDelegator extends TempletonDelegator {
     public static final String STREAM_CLASS = TempletonStreamJob.class.getName();

    public StreamingDelegator(AppConfig appConf, ExecService execService) {
        super(appConf, execService);
    }

    public EnqueueBean run(String user,
                           List<String> inputs, String output,
                           String mapper, String reducer,
                           List<String> files, List<String> defines,
                           List<String> cmdenvs,
                           List<String> jarArgs)
        throws NotAuthorizedException, BusyException, QueueException,
        ExecuteException, IOException
    {
        ArrayList<String> args = new ArrayList<String>();
        args.add("jar");
        args.add(appConf.templetonJar());
        args.add(STREAM_CLASS);
        args.add("-libjars");
        args.add(appConf.streamingJar());
        for (String input : inputs) {
            args.add("-input");
            args.add(input);
        }
        args.add("-output");
        args.add(output);
        args.add("-mapper");
        args.add(mapper);
        args.add("-reducer");
        args.add(reducer);

        for (String f : files)
            args.add("-file" + f);
        for (String d : defines)
            args.add("-D" + d);
        for (String e : cmdenvs)
            args.add("-cmdenv" + e);
        args.addAll(jarArgs);

        HashMap<String, String> env = new HashMap<String, String>();
        env.put("HADOOP_CLASSPATH", appConf.streamingJar());

        ExecBean exec = execService.run(user, appConf.clusterHadoop(), args, env);
        if (exec.exitcode != 0)
            throw new QueueException("invalid exit code", exec);
        String id = TempletonUtils.extractJobId(exec.stdout);
        if (id == null)
            throw new QueueException("Unable to get job id", exec);

        return new EnqueueBean(id, exec);
    }
}
