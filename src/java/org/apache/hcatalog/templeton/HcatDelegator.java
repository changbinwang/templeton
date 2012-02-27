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
import java.util.Map;
import java.util.HashMap;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hcatalog.templeton.tool.TempletonUtils;

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
        String res = jsonRun(user, exec, group, permissions);
        return JsonBuilder.create(res)
            .put("database", db)
            .put("table", table)
            .build();
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

        String res = jsonRun(user, exec, group, permissions);
        if (single)
            res = singleTable(res);
        return JsonBuilder.create(res)
            .remove("tableName")
            .put("database", db)
            .put("table", table)
            .build();
    }

    // Pull out the first table from the "show extended" json.
    private String singleTable(String json)
        throws IOException
    {
        Map obj = JsonBuilder.jsonToMap(json);
        List tables = (List) obj.get("tables");
        if (tables == null || tables.size() == 0)
            return json;
        return JsonBuilder.mapToJson(tables.get(0));
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
        String res = jsonRun(user, exec, group, permissions);
        return JsonBuilder.create(res)
            .put("database", db)
            .put("table", table)
            .build();
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
        String res = singleTable(jsonRun(user, exec, group, permissions));
        return JsonBuilder.create(res)
            .remove("tableName")
            .put("database", db)
            .put("table", table)
            .put("partition", partition)
            .build();
    }

    /**
     * Add one partition.
     */
    public String addOnePartition(String user, String db, String table,
                                  PartitionDesc desc,
                                  String group, String permissions)
        throws NotAuthorizedException, BusyException, ExecuteException, IOException
    {
        String exec = String.format("use %s; alter table %s add partition (%s)",
                                    db, table, desc.partition);
        if (TempletonUtils.isset(desc.location))
            exec += String.format(" location '%s'", desc.location);
        exec += ";";
        String res = jsonRun(user, exec, group, permissions);
        return JsonBuilder.create(res)
            .remove("tableName")
            .put("database", db)
            .put("table", table)
            .put("partition", desc.partition)
            .build();
    }

    /**
     * Run an hcat expression and return just the json outout.
     */
    private String jsonRun(String user, String exec, String group, String permissions)
        throws NotAuthorizedException, BusyException, ExecuteException, IOException
    {
        ExecBean res = run(user, exec, true, group, permissions);
        // System.err.println("raw hcat result: " + JsonBuilder.mapToJson(res));

        if (res != null && res.exitcode == 0)
            return res.stdout;
        else
            return null;
    }
}
