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
import java.util.List;
import org.apache.hcatalog.templeton.tool.JobState;
import org.apache.hcatalog.templeton.tool.TempletonControllerJob;
import org.apache.hcatalog.templeton.tool.TempletonUtils;

/**
 * The helper class for all the Templeton delegator classes that
 * launch child jobs using sudo.
 */
public class LauncherDelegator extends TempletonDelegator {
    public static final String JAR_CLASS = TempletonControllerJob.class.getName();

    public LauncherDelegator(AppConfig appConf, ExecService execService) {
        super(appConf, execService);
    }

    public void registerJob(String id, String user, String callback)
        throws IOException
    {
        JobState state = new JobState(id, appConf);
        state.setUser(user);
        state.setCallback(callback);
    }

    public List<String> makeLauncherArgs(AppConfig appConf, String statusdir,
                                         List<String> copyFiles)
    {
        ArrayList<String> args = new ArrayList<String>();

        args.add("jar");
        args.add(appConf.templetonJar());
        args.add(JAR_CLASS);
        args.add("-libjars");
        args.add(appConf.libJars());

        addDef(args, JobState.ZK_HOSTS, appConf.get(JobState.ZK_HOSTS));
        addDef(args, JobState.ZK_SESSION_TIMEOUT,
               appConf.get(JobState.ZK_SESSION_TIMEOUT));
        addDef(args, TempletonControllerJob.STATUSDIR_NAME, statusdir);
        addDef(args, TempletonControllerJob.COPY_NAME,
               TempletonUtils.encodeArray(copyFiles));

        return args;
    }

    public static void addDef(List<String> args, String name, String val) {
        if (val != null) {
            args.add("-D");
            args.add(name + "=" + val);
        }
    }
}
