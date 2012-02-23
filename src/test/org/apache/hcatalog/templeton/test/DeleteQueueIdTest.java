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

import junit.framework.TestCase;

import org.apache.hcatalog.templeton.BadParam;
import org.apache.hcatalog.templeton.Main;
import org.apache.hcatalog.templeton.NotAuthorizedException;
import org.apache.hcatalog.templeton.SimpleWebException;
import org.apache.hcatalog.templeton.test.mock.MockServer;

/*
 * Test that the server describeTable() call works.
 */
public class DeleteQueueIdTest extends TestCase {

    MockServer server;

    public void setUp() {
        new Main(null);         // Initialize the config
        server = new MockServer();
    }
    
    public void testDeleteQueueId() {
        try {
            try {
                server.deleteQueueId(null, "admin");
                fail("null param succeeded.");
            } catch (BadParam bp) {
                // Success
            }
            try {
                server.deleteQueueId("fred", null);
                fail("null user succeeded.");
            } catch (NotAuthorizedException bp) {
                // Success
            }
       
            try {
                server.deleteQueueId("fred", "admin");
            } catch (SimpleWebException swe) {
                // hdfs isn't hooked up right
            } catch (Exception e) {
                // hdfs isn't hooked up
            }
        } catch (SimpleWebException swe) {
            swe.printStackTrace();
            fail("describe table execution caused a failure");
        } catch (Exception e) {
            e.printStackTrace();
            fail("describe table execution caused a failure");
        }
    }
}
