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
 * Test the server ddl() request.
 */
public class DDLTest extends TestCase {

    MockServer server;

    public void setUp() {
        new Main(null);         // Initialize the config
        server = new MockServer();
    }

    public void testDDL() {
        try {
            try {
                server.ddl(null, "admin", "777", "admin");
                fail("null param succeeded.");
            } catch (BadParam bp) {
                // Success
            }
            try {
                server.ddl("show tables", "admin", "777", null);
                fail("null user succeeded.");
            } catch (NotAuthorizedException ex) {
                // Success
            }
            validateDDL(server, "show tables", "admin", "777");
            validateDDL(server, "", "", "");
            validateDDL(server, "show tables", "", "");
            validateDDL(server, "show tables", "admin", "");
        } catch (SimpleWebException swe) {
            swe.printStackTrace();
            fail("ddl execution caused a failure");
        } catch (Exception e) {
            e.printStackTrace();
            fail("ddl execution caused a failure");
        }
    }

    public void validateDDL(MockServer server, String command, String group,
                            String permissions)
        throws SimpleWebException, IOException
    {
        ExecBean bean = server.ddl(command, group, permissions, server.getUser());
        assertTrue(bean.stdout.endsWith("bin/hcat"));
        String tmp = bean.stderr.substring(bean.stderr.indexOf("[") + 1,
                                           bean.stderr.indexOf("]"));
        String[] parts = tmp.split(",");
        assertTrue(parts[0].trim().equals("-e"));
        assertTrue(parts[1].trim().equals(command));
        assertTrue(parts[2].trim().equals("-g"));
        assertTrue(parts[3].trim().equals(group));
        assertTrue(parts[4].trim().equals("-p"));
        assertTrue(parts[5].trim().equals(permissions));
    }
}
