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
import org.apache.hadoop.util.StringUtils;

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


    /**
     * Encode a command line argument.  We need to allow for empty
     * arguments.
     */
    public static String encodeCliArg(String s) {
        if (TempletonUtils.isset(s))
            return "*" + s;
        else
            return "*";
    }

    /**
     * Decode a command line argument.  We need to allow for empty
     * arguments.
     */
    public static String decodeCliArg(String s) {
        if (s != null && s.startsWith("*"))
            return s.substring(1);
        else
            return s;
    }

    /**
     * Take an array of strings and encode it into one string.
     */
    public static String encodeArray(String[] plain) {
        if (plain == null)
            return null;

        String[] escaped = new String[plain.length];

        for (int i = 0; i < plain.length; ++i)
            escaped[i] = StringUtils.escapeString(plain[i]);

        return StringUtils.arrayToString(escaped);
    }

    /**
     * Take an encode strings and decode it into an array of strings.
     */
    public static String[] decodeArray(String s) {
        if (s == null)
            return null;

        String[] escaped = StringUtils.split(s);
        String[] plain = new String[escaped.length];

        for (int i = 0; i < escaped.length; ++i)
            plain[i] = StringUtils.unEscapeString(escaped[i]);

        return plain;
    }

    /**
     * Encode an array to be used on the command line.
     */
    public static String encodeCliArray(String[] array) {
        String x = encodeArray(array);
        return encodeCliArg(x);
    }

    /**
     * Encode a string as a one element array to be used on the
     * command line.
     */
    public static String encodeCliArray(String s) {
        if (s == null)
            return null;

        String[] array = new String[1];
        array[0] = s;
        return encodeCliArray(array);
    }

    /**
     * Decode a command line arg into an array of strings.
     */
    public static String[] decodeCliArray(String s) {
        String x = decodeCliArg(s);
        return decodeArray(x);
    }
}
