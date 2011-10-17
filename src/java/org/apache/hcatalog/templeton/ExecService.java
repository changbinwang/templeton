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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;

/**
 * Execute a local program.  This is a singelton service that will
 * execute authorized programs as non-privileged users on the local
 * box.  We do allow hadoop in the list of authorized programs, so
 * don't rely on authorized programs as a security feature.  See
 * ExecService.run and ExecService.runUnlimited for details.
 */
public class ExecService {
    public static final String ENCODING = "UTF-8";
    public static final int TIMEOUT_MS = 10 * 1000;
    public static final int MAX_EXECS = 16;

    public static final String SUDO = "/usr/bin/sudo";

    public static final String HCAT = "hcat";
    public static final String HADOOP = "hadoop";
    public static HashMap<String, String> authorizedPrograms
        = new HashMap<String, String>() {{
            put(HCAT, "/usr/local/hcat/bin/hcat");
            put(HADOOP, System.getenv("HADOOP_HOME") + "/bin/hadoop");
            put("date", "/bin/date");
        }};

    public static final String[] SUDO_ENV_VARS = {"HADOOP_HOME", "JAVA_HOME"};

    private static volatile ExecService theSingleton;

    /**
     * Retrieve the singleton.
     */
    public static synchronized ExecService getInstance() {
        if (theSingleton == null) {
            theSingleton = new ExecService();
        }
        return theSingleton;
    }

    private Semaphore avail;

    private ExecService() {
        avail = new Semaphore(MAX_EXECS);
    }

    /**
     * Run the program synchronously as the given user. We rate limit
     * the number of processes that can simultaneously created for
     * this instance.
     *
     * @param user      A valid user
     * @param program   The program name to run
     * @param env       Any extra environment variables to set
     * @returns         The result of the run.
     */
    public ExecBean run(String user, String program, List<String> args,
                        Map<String, String> env)
        throws NotAuthorizedException, BusyException, ExecuteException, IOException
    {
        boolean aquired = false;
        try {
            aquired = avail.tryAcquire();
            if (aquired) {
                return runUnlimited(user, program, args, env);
            } else {
                throw new BusyException();
            }
        } finally {
            if (aquired) {
                avail.release();
            }
        }
    }

    /**
     * Run the program synchronously as the given user.  Warning:
     * CommandLine will trim the argument strings.  Only authorized
     * programs may be run.
     *
     * @param user      A valid user
     * @param program   The program name to run.
     * @returns         The result of the run.
     */
    public ExecBean runUnlimited(String user, String program, List<String> args,
                                 Map<String, String> env)
        throws NotAuthorizedException, ExecuteException, IOException
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

        CommandLine cmd = makeCommandLine(user, program, args, env);

        System.err.println("--- Running: " + cmd);
        ExecBean res = new ExecBean();
        res.exitCode = executor.execute(cmd);
        res.stdout = outStream.toString(ENCODING);
        res.stderr = errStream.toString(ENCODING);

        return res;
    }

    private CommandLine makeCommandLine(String user, String program, List<String> args,
                                        Map<String, String> env)
        throws NotAuthorizedException, IOException
    {
        String path = authedProgram(program);

        CommandLine cmd = new CommandLine(new File(SUDO));
        cmd.addArgument("-u");
        cmd.addArgument(user);

        for (Map.Entry entry : sudoEnv(env).entrySet()) {
            cmd.addArgument(entry.getKey() + "=" + entry.getValue());
        }

        cmd.addArgument(path);
        if (args != null)
            for (String arg : args)
                cmd.addArgument(arg, false);

        return cmd;
    }

    /**
     * Build the environment used for all sudo calls.
     *
     * @return The environment variables.
     */
    public Map<String, String> sudoEnv(Map<String, String> env) {
        HashMap<String, String> res = new HashMap<String, String>();

        for (String key : SUDO_ENV_VARS) {
            String val = System.getenv(key);
            if (val != null)
                res.put(key, val);
        }
        if (env != null)
            res.putAll(env);

        return res;
    }

    /**
     * Given an authorized program name, lookup the fully qualified
     * path.  Throws an exception if the program is missing or not
     * authorized.
     *
     * @param program   The name of the program without '/'
     * @return          The path of the program.
     */
    public String authedProgram(String program)
        throws NotAuthorizedException, IOException
    {
        String path = authorizedPrograms.get(program);
        if (path == null) {
            throw new NotAuthorizedException("Unauthorized program: " + program);
        }

        File f = new File(path);
        if (f.canExecute()) {
            return f.getCanonicalPath();
        } else {
            throw new NotAuthorizedException("Unable to access program: " + program);
        }
    }
}
