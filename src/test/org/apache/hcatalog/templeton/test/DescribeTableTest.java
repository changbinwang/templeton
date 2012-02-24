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

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hcatalog.templeton.BadParam;
import org.apache.hcatalog.templeton.ExecBean;
import org.apache.hcatalog.templeton.Main;
import org.apache.hcatalog.templeton.NotAuthorizedException;
import org.apache.hcatalog.templeton.SimpleWebException;
import org.apache.hcatalog.templeton.test.mock.MockServer;

/*
 * Test that the server describeTable() call works.
 */
public class DescribeTableTest extends TestCase {

    MockServer server;

    public void setUp() {
        new Main(null);         // Initialize the config
        server = new MockServer();
    }
    
    public void testDescribeTable() {
        try {
            try {
                server.describeTable(null, "joe", "admin");
                fail("null param succeeded.");
            } catch (BadParam bp) {
                // Success
            }
            try {
                server.describeTable("fred", null, "admin");
                fail("null param succeeded.");
            } catch (BadParam bp) {
                // Success
            }
       
            try {
                server.describeTable("invalid db name!", "joe", "admin");
                fail("weird db name succeeded.");
            } catch (BadParam bp) {
                // Success
            }
            validateTables("fred", "joe", "admin");
            try {
                server.describeTable("fred", "joe", null);
                fail("null user succeeded");
            } catch (NotAuthorizedException ne) {
                // Success
            }
        } catch (SimpleWebException swe) {
            swe.printStackTrace();
            fail("describe table execution caused a failure");
        } catch (Exception e) {
            e.printStackTrace();
            fail("describe table execution caused a failure");
        }
    }
   
    public void validateTables(String db, String table,
            String user)
      throws SimpleWebException, IOException
      {
        ExecBean bean = server.describeTable
                (db, table, user);
        System.out.println("out: " + bean.stdout);
        System.out.println("err: " + bean.stderr);
        
        assertTrue(bean.stdout.endsWith("bin/hcat"));
        String tmp = bean.stderr.substring(bean.stderr.indexOf("[") + 1,
                                   bean.stderr.indexOf("]"));
        String[] parts = tmp.split(",");
        assertTrue(parts[0].trim().equals("-e"));
        assertTrue(parts[1].trim().indexOf("use " + db + "; desc " + table) > -1);
      }
}
