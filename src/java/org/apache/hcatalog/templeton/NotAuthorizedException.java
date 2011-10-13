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
import java.util.HashMap;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Simple "user not found" type exception.
 */
public class NotAuthorizedException extends WebApplicationException {
    public NotAuthorizedException(String msg) {
        super(buildMessage(msg));
    }

    public static Response buildMessage(String msg) {
        HashMap err = new HashMap<String,String>();
        err.put("error", msg);
        String json = "\"error\"";
        try {
            json = new ObjectMapper().writeValueAsString(err);
        } catch(IOException e) {
        }

        return Response.status(400)
            .entity(json)
            .type(MediaType.APPLICATION_JSON)
            .build();
    }
}
