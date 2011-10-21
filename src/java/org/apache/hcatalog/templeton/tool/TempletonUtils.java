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
package org.apache.hcatalog.templeton.tool;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.hadoop.mapreduce.JobID;

/**
 * General utility methods.
 */
public class TempletonUtils {
    public static final String JOB_ID_TAG = "templeton-job-id:";

    /**
     * Is the object non-empty?
     */
    public static boolean isset(String s) {
        return (s != null) && (s.length() > 0);
    }

    /**
     * Is the object non-empty?
     */
    public static <T> boolean isset(T[] a) {
        return (a != null) && (a.length > 0);
    }


    /**
     * Print a job id for later extraction.
     */
    public static void printTaggedJobID(JobID jobid) {
        System.out.println();
        System.out.println(JOB_ID_TAG + jobid);
    }

    /**
     * Extract the job id that we output earlier.
     */
    public static String extractJobId(String stdout) {
        String pat = "^" + Pattern.quote(JOB_ID_TAG) + "(\\S+)";
        Pattern re = Pattern.compile(pat, Pattern.MULTILINE);

        Matcher m = re.matcher(stdout);
        if (! m.find())
            return null;
        return m.group(1);
    }
}
