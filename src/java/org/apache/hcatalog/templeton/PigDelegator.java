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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.exec.ExecuteException;
import org.apache.hcatalog.templeton.tool.TempletonControllerJob;
import org.apache.hcatalog.templeton.tool.TempletonUtils;

/**
 * Submit a Pig job.  We do this by running the hadoop executable
 * on the local server using the ExecService.  This allows us to
 * easily verify that the user identity is being securely used.
 *
 * This is the backend of the pig web service.
 */
public class PigDelegator extends TempletonDelegator {
    public static final String JAR_CLASS = TempletonControllerJob.class.getName();

    public PigDelegator(AppConfig appConf, IExecService execService) {
        super(appConf, execService);
    }

    public EnqueueBean run(String user,
                           String execute, String srcFile,
                           List<String> pigArgs, String otherFiles,
                           String statusdir)
        throws NotAuthorizedException, BadParam, BusyException, QueueException,
        ExecuteException, IOException
    {
        List<String> args = makeArgs(execute,
                                     srcFile, pigArgs,
                                     otherFiles, statusdir);

        ExecBean exec = execService.run(user, appConf.clusterHadoop(), args, null);
        if (exec.exitcode != 0)
            throw new QueueException("invalid exit code", exec);
        String id = TempletonUtils.extractJobId(exec.stdout);
        if (id == null)
            throw new QueueException("Unable to get job id", exec);

        return new EnqueueBean(id, exec);
    }

    private List<String> makeArgs(String execute, String srcFile,
                                  List<String> pigArgs, String otherFiles,
                                  String statusdir)
        throws BadParam, IOException
    {
        ArrayList<String> args = new ArrayList<String>();
        try {
            args.add("jar");
            args.add(appConf.templetonJar());
            args.add(JAR_CLASS);
            args.add("-archives");
            args.add(appConf.pigArchive());

            ArrayList<String> allFiles = new ArrayList<String>();
            if (TempletonUtils.isset(srcFile))
                allFiles.add(TempletonUtils.hadoopFsFilename(srcFile, appConf));
            if (TempletonUtils.isset(otherFiles)) {
                String[] ofs = TempletonUtils.hadoopFsListAsArray(otherFiles, appConf);
                allFiles.addAll(Arrays.asList(ofs));
            }

            args.add(TempletonUtils.encodeCliArray(allFiles));
            args.add(TempletonUtils.encodeCliArg(statusdir));
            args.add("--");
            args.add(appConf.pigPath());
            if (TempletonUtils.isset(execute)) {
                args.add("-execute");
                args.add(execute);
            } else if (TempletonUtils.isset(srcFile)) {
                args.add("-file");
                args.add(TempletonUtils.hadoopFsPath(srcFile, appConf).getName());
            }
            args.addAll(pigArgs);
        } catch (FileNotFoundException e) {
            throw new BadParam(e.getMessage());
        } catch (URISyntaxException e) {
            throw new BadParam(e.getMessage());
        }

        return args;
    }
}
