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
import java.util.Map;
import org.apache.commons.exec.ExecuteException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hcatalog.templeton.tool.JobState;
import org.apache.hcatalog.templeton.tool.TempletonControllerJob;
import org.apache.hcatalog.templeton.tool.TempletonStorage;
import org.apache.hcatalog.templeton.tool.TempletonUtils;
import org.apache.hcatalog.templeton.tool.ZooKeeperStorage;

/**
 * The helper class for all the Templeton delegator classes that
 * launch child jobs.
 */
public class LauncherDelegator extends TempletonDelegator {
    public static final String JAR_CLASS = TempletonControllerJob.class.getName();
    protected String runAs = null;

    public LauncherDelegator(AppConfig appConf, ExecService execService) {
        super(appConf, execService);
    }

    public void registerJob(String id, String user, String callback)
        throws IOException
    {
        JobState state = null;
        try {
            state = new JobState(id, Main.getAppConfigInstance());
            state.setUser(user);
            state.setCallback(callback);
        } finally {
            if (state != null)
                state.close();
        }
    }

    /**
     * Enqueue the TempletonControllerJob by running the hadoop
     * executable.
     */
    public EnqueueBean enqueueController(String user, String callback,
                                         List<String> args)
        throws NotAuthorizedException, BusyException, ExecuteException,
        IOException, QueueException
    {
        // Setup the hadoop vars to specify the user.
        Map<String, String> env = TempletonUtils.hadoopUserEnv(user, null);

        // Run the job
        ExecBean exec = execService.run(appConf.clusterHadoop(), args, env);

        // Return the job info
        if (exec.exitcode != 0)
            throw new QueueException("invalid exit code", exec);
        String id = TempletonUtils.extractJobId(exec.stdout);
        if (id == null)
            throw new QueueException("Unable to get job id", exec);
        registerJob(id, user, callback);

        return new EnqueueBean(id, exec);
    }

    public List<String> makeLauncherArgs(AppConfig appConf, String statusdir,
                                         String completedUrl,
                                         List<String> copyFiles)
    {
        ArrayList<String> args = new ArrayList<String>();

        args.add("jar");
        args.add(appConf.templetonJar());
        args.add(JAR_CLASS);
        args.add("-libjars");
        args.add(appConf.libJars());
        addCacheFiles(args, appConf);

        // Set user
        addDef(args, "user.name", runAs);

        // Storage vars
        addDef(args, TempletonStorage.STORAGE_CLASS,
               appConf.get(TempletonStorage.STORAGE_CLASS));
        addDef(args, TempletonStorage.STORAGE_ROOT,
                appConf.get(TempletonStorage.STORAGE_ROOT));
        addDef(args, ZooKeeperStorage.ZK_HOSTS,
                appConf.get(ZooKeeperStorage.ZK_HOSTS));
        addDef(args, ZooKeeperStorage.ZK_SESSION_TIMEOUT,
                appConf.get(ZooKeeperStorage.ZK_SESSION_TIMEOUT));

        // Completion notifier vars
        addDef(args, AppConfig.HADOOP_END_RETRY_NAME,
               appConf.get(AppConfig.CALLBACK_RETRY_NAME));
        addDef(args, AppConfig.HADOOP_END_INTERVAL_NAME,
               appConf.get(AppConfig.CALLBACK_INTERVAL_NAME));
        addDef(args, AppConfig.HADOOP_END_URL_NAME, completedUrl);

        // Internal vars
        addDef(args, TempletonControllerJob.STATUSDIR_NAME, statusdir);
        addDef(args, TempletonControllerJob.COPY_NAME,
               TempletonUtils.encodeArray(copyFiles));
        addDef(args, TempletonControllerJob.OVERRIDE_CLASSPATH,
               makeOverrideClasspath(appConf));

        return args;
    }

    /**
     * Add files to the Distributed Cache for the controller job.
     */
    public static void addCacheFiles(List<String> args, AppConfig appConf) {
        String overrides = appConf.overrideJarsString();
        if (overrides != null) {
            args.add("-files");
            args.add(overrides);
        }
    }

    /**
     * Create the override classpath, which will be added to
     * HADOOP_CLASSPATH at runtime by the controller job.
     */
    public static String makeOverrideClasspath(AppConfig appConf) {
        String[] overrides = appConf.overrideJars();
        if (overrides == null)
            return null;

        ArrayList<String> cp = new ArrayList<String>();
        for (String fname : overrides) {
            Path p = new Path(fname);
            cp.add(p.getName());
        }
        return StringUtils.join(":", cp);
    }


    /**
     * Add a Hadoop command line definition to args if the value is
     * not null.
     */
    public static void addDef(List<String> args, String name, String val) {
        if (val != null) {
            args.add("-D");
            args.add(name + "=" + val);
        }
    }

}
