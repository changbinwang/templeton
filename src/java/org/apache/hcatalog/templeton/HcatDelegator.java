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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.exec.ExecuteException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Run hcat on the local server using the ExecService.  This is
 * the backend of the ddl web service.
 */
public class HcatDelegator extends LauncherDelegator {
    public HcatDelegator(AppConfig appConf, ExecService execService) {
        super(appConf, execService);
    }

    /**
     * Run the local hcat executable.
     */
    public ExecBean run(String exec, boolean format, String group, String permissions)
        throws NotAuthorizedException, BusyException, ExecuteException, IOException
    {
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
        if (format) {
            args.add("-D");
            args.add("hive.format=json");
        }

        return execService.run(appConf.clusterHcat(), args, null);
    }

    /**
     * Return a json description of the table.
     */
    public String describeTable(String db, String table, boolean extended,
                                String group, String permissions)
        throws NotAuthorizedException, BusyException, ExecuteException, IOException
    {
        String exec = "use " + db + "; ";
        if (extended)
            exec += "desc extended " + table + "; ";
        else
            exec += "desc " + table + "; ";
        return jsonRun(exec, group, permissions);
    }

    /**
     * Return a json "show table like".  This will return a list of
     * tables, unless single is true.
     */
    public String showTable(String db, String table, boolean extended,
                            String group, String permissions, boolean single)
        throws NotAuthorizedException, BusyException, ExecuteException, IOException
    {
        String exec = "use " + db + "; ";
        if (extended)
            exec += "show table extended like " + table + "; ";
        else
            exec += "show table like  " + table + "; ";

        String show = jsonRun(exec, group, permissions);

        if (! single)
            return show;
        else {
            try {
                ObjectMapper mapper = new ObjectMapper();
                Map<String, Object> obj = mapper.readValue(show, Map.class);
                List tables = (List) obj.get("tables");
                if (tables == null || tables.size() == 0)
                    return show;
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                mapper.writeValue(out, tables.get(0));
                return out.toString();
            } catch (Exception e) {
                return show;
            }
        }
    }

    /**
     * Run an hcat expression and return just the json outout.
     */
    private String jsonRun(String exec, String group, String permissions)
        throws NotAuthorizedException, BusyException, ExecuteException, IOException
    {
        ExecBean res = run(exec, true, group, permissions);
        if (res != null && res.exitcode == 0)
            return res.stdout;
        else
            return null;
    }


}
