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
import java.util.HashMap;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hcatalog.templeton.tool.TempletonUtils;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Run hcat on the local server using the ExecService.  This is
 * the backend of the ddl web service.
 */
public class HcatDelegator extends LauncherDelegator {
    private static final Log LOG = LogFactory.getLog(HcatDelegator.class);

    public HcatDelegator(AppConfig appConf, ExecService execService) {
        super(appConf, execService);
    }

    /**
     * Run the local hcat executable.
     */
    public ExecBean run(String user, String exec, boolean format,
                        String group, String permissions)
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

        // Setup the hadoop vars to specify the user.
        String cp = makeOverrideClasspath(appConf);
        Map<String, String> env = TempletonUtils.hadoopUserEnv(user, cp);
        return execService.run(appConf.clusterHcat(), args, env);
    }

    /**
     * Return a json description of the table.
     */
    public String describeTable(String user, String db, String table, boolean extended,
                                String group, String permissions)
        throws NotAuthorizedException, BusyException, ExecuteException, IOException
    {
        String exec = "use " + db + "; ";
        if (extended)
            exec += "desc extended " + table + "; ";
        else
            exec += "desc " + table + "; ";
        return jsonRun(user, exec, group, permissions);
    }

    /**
     * Return a json "show table like".  This will return a list of
     * tables, unless single is true.
     */
    public String showTable(String user, String db, String table, boolean extended,
                            String group, String permissions, boolean single)
        throws NotAuthorizedException, BusyException, ExecuteException, IOException
    {
        String exec = "use " + db + "; ";
        if (extended)
            exec += "show table extended like " + table + "; ";
        else
            exec += "show table like  " + table + "; ";

        String show = jsonRun(user, exec, group, permissions);

        if (! single)
            return show;
        else
            return singleTable(show);
    }

    // Pull out the first table from the "show extended" json.
    private String singleTable(String json) {
        try {
            Map obj = jsonToMap(json);
            List tables = (List) obj.get("tables");
            if (tables == null || tables.size() == 0)
                return json;
            return mapToJson(tables.get(0));
        } catch (Exception e) {
            return json;
        }
    }

    /**
     * Return a json description of the partitions.
     */
    public String showPartitions(String user, String db, String table,
                                 String group, String permissions)
        throws NotAuthorizedException, BusyException, ExecuteException, IOException
    {
        String exec = "use " + db + "; ";
        exec += "show partitions " + table + "; ";
        return jsonRun(user, exec, group, permissions);
    }

    /**
     * Return a json description of one partition.
     */
    public String showOnePartition(String user, String db, String table, String partition,
                                   String group, String permissions)
        throws NotAuthorizedException, BusyException, ExecuteException, IOException
    {
        String exec = "use " + db + "; ";
        exec += "show table extended like " + table
            + " partition (" + partition + "); ";
        return singleTable(jsonRun(user, exec, group, permissions));
    }

    /**
     * Run an hcat expression and return just the json outout.
     */
    private String jsonRun(String user, String exec, String group, String permissions)
        throws NotAuthorizedException, BusyException, ExecuteException, IOException
    {
        ExecBean res = run(user, exec, true, group, permissions);
        if (res != null && res.exitcode == 0)
            return res.stdout;
        else
            return null;
    }

    private Map jsonToMap(String json)
        throws IOException
    {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(json, Map.class);
    }

    private String mapToJson(Object obj)
        throws IOException
    {
        ObjectMapper mapper = new ObjectMapper();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        mapper.writeValue(out, obj);
        return out.toString();
    }
}
