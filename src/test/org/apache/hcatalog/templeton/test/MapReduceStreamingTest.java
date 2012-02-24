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
package org.apache.hcatalog.templeton.test;

import java.util.ArrayList;

import junit.framework.TestCase;

import org.apache.hcatalog.templeton.BadParam;
import org.apache.hcatalog.templeton.Main;
import org.apache.hcatalog.templeton.NotAuthorizedException;
import org.apache.hcatalog.templeton.SimpleWebException;
import org.apache.hcatalog.templeton.test.mock.MockServer;

/*
 * Test that the server code exists.
 */
public class MapReduceStreamingTest extends TestCase {

    MockServer server;
    ArrayList<String> inputs = new ArrayList<String>();
    String output = "fred2";
    String mapper = "fred3";
    String reducer = "fred4";
    ArrayList<String> files = new ArrayList<String>();
    ArrayList<String> defines = new ArrayList<String>();
    ArrayList<String> cmdenvs = new ArrayList<String>();
    ArrayList<String> args = new ArrayList<String>();
    String statusdir = "fred5";
    String callback = "fred6";
    String user = "admin";

    public void setUp() {
        new Main(null);         // Initialize the config
        server = new MockServer();
        inputs.add("fred1");
    }

    public void testMapReduceStreaming() {
        try {
            try {
                server.mapReduceStreaming(null, output, mapper, reducer,
                        files, defines, cmdenvs, args, statusdir, callback,
                        user);
                fail("Null inputs were allowed.");
            } catch (BadParam bp) {
                // Success
            }
            try {
                server.mapReduceStreaming(new ArrayList<String>(), 
                        output, mapper, reducer,
                        files, defines, cmdenvs, args, statusdir, callback,
                        user);
                fail("Empty inputs were allowed.");
            } catch (BadParam bp) {
                // Success
            }
            try {
                server.mapReduceStreaming(inputs, output, mapper, null,
                        files, defines, cmdenvs, args, statusdir, callback,
                        user);
                fail("Null reducer was allowed.");
            } catch (BadParam bp) {
                // Success
            }
            try {
                server.mapReduceStreaming(inputs, output, null, reducer,
                        files, defines, cmdenvs, args, statusdir, callback,
                        user);
                fail("Null mapper was allowed.");
            } catch (BadParam bp) {
                // Success
            }
            try {
                server.mapReduceStreaming(inputs, output, mapper, reducer,
                        files, defines, cmdenvs, args, statusdir, callback,
                        null);
                fail("Null user was allowed.");
            } catch (NotAuthorizedException bp) {
                // Success
            }
            try {
                server.mapReduceStreaming(inputs, output, mapper, reducer,
                    files, defines, cmdenvs, args, statusdir, callback,
                    user);
                // Hey, cool, hdfs is set up and running.
            } catch (SimpleWebException swe) {
                // hdfs isn't hooked up right
            } catch (Exception e) {
                // hdfs isn't hooked up
            }
        } catch (SimpleWebException swe) {
            swe.printStackTrace();
            fail("mapreducestreaming execution caused a failure");
        } catch (Exception e) {
            e.printStackTrace();
            fail("mapreducestreaming execution caused a failure");
        }
    }
}
