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
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.commons.exec.ExecuteException;

/**
 * The Templeton Web API server.
 */
@Path("/v1")
public class Server {
    public static final String STATUS_MSG
        = "{\"status\": \"ok\", \"version\": \"v1\"}\n";
    public static final String USER_PARAM = "user.name";

    private static ExecService execService = ExecService.getInstance();

    /**
     * Check the status of this server.
     */
    @GET
    @Path("status.json")
    @Produces({MediaType.APPLICATION_JSON})
    public String status() {
        return STATUS_MSG;
    }

    /**
     * Exececute a program on the local box.  It is run as the
     * authenticated user, limited to a small set of programs, and
     * rate limited.
     */
    @POST
    @Path("exec.json")
    @Produces({MediaType.APPLICATION_JSON})
    public ExecBean exec(@FormParam(USER_PARAM) String user,
                         @FormParam("program") String program,
                         @FormParam("arg") List<String> args)
        throws NotAuthorizedException, BusyException, BadParam,
               ExecuteException, IOException
    {
        verifyUser(user);
        verifyParam(program, "program");
        return execService.run(user, program, args);
    }

    /**
     * Exececute an hcat ddl expression on the local box.  It is run
     * as the authenticated user and rate limited.
     */
    @POST
    @Path("ddl.json")
    @Produces({MediaType.APPLICATION_JSON})
    public ExecBean ddl(@FormParam(USER_PARAM) String user,
                        @FormParam("exec") String exec,
                        @FormParam("group") String group,
                        @FormParam("permissions") String permissions)
        throws NotAuthorizedException, BusyException, BadParam,
               ExecuteException, IOException
    {
        verifyUser(user);
        verifyParam(exec, "exec");

        ArrayList<String> args = new ArrayList<String>();
        args.add("-e");
        args.add(exec);
        if (group != null) {
            args.add("-g");
            args.add(group);
        }
        if (permissions != null) {
            args.add("-p");
            args.add(permissions);
        }

        return execService.run(user, ExecService.HCAT, args);
    }

    /**
     * Verify that we have a valid user.  Throw an exception if invalid.
     */
    public void verifyUser(String user)
        throws NotAuthorizedException
    {
        if (user == null) {
            throw new NotAuthorizedException("missing " + USER_PARAM + " parameter");
        }
    }

    /**
     * Verify that the parameter exists.  Throw an exception if invalid.
     */
    public void verifyParam(String param, String name)
        throws BadParam
    {
        if (param == null) {
            throw new BadParam("Missing " + name + " parameter");
        }
    }
}
