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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;

/**
 * Execute a local program.
 */
public class ExecService {
    public static final String ENCODING = "UTF-8";
    public static final int TIMEOUT_MS = 10 * 1000;

    public ExecService() {}

    /**
     * Run the program synchronously as the given user.  Warning:
     * CommandLine will trim the argument strings.
     *
     * @param user      A valid user
     * @param program   The program name to run
     * @returns         The result of the run.
     */
    public ExecBean run(String user, String program, List<String> args)
        throws ExecuteException, IOException
    {
        DefaultExecutor executor = new DefaultExecutor();
        executor.setExitValues(null);

        // Setup stdout and stderr
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        ByteArrayOutputStream errStream = new ByteArrayOutputStream();
        executor.setStreamHandler(new PumpStreamHandler(outStream, errStream));

        // Only run for N seconds
        ExecuteWatchdog watchdog = new ExecuteWatchdog(TIMEOUT_MS);
        executor.setWatchdog(watchdog);

        CommandLine cmd = new CommandLine(new File(program));
        for (String arg : args) {
            cmd.addArgument(arg, false);
        }

        ExecBean res = new ExecBean();
        res.exitCode = executor.execute(cmd);
        res.stdout = outStream.toString(ENCODING);
        res.stderr = errStream.toString(ENCODING);

        return res;
    }
}
