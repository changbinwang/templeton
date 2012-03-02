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
import org.apache.hadoop.util.StringUtils;
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
        if (TempletonUtils.isset(group)) {
            args.add("-g");
            args.add(group);
        }
        if (TempletonUtils.isset(permissions)) {
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
     * Return a json description of the database.
     */
    public String describeDatabase(String user, String db, boolean extended,
                                   String group, String permissions)
        throws HcatException, NotAuthorizedException, BusyException,
            ExecuteException, IOException
    {
        String exec = "desc database " + db + "; ";
        if (extended) {
            exec = "desc database extended " + db + "; ";
        }
        String res = jsonRun(user, exec, group, permissions);
        return JsonBuilder.create(res)
            .put("database", db)
            .build();
    }

    /**
     * Return a json "show databases like".  This will return a list of
     * databases.
     */
    public String showDatabases(String user, String dbPattern,
                             String group, String permissions)
        throws HcatException, NotAuthorizedException, BusyException,
        ExecuteException, IOException
    {
        String exec = String.format("show databases like '%s';",
                                    dbPattern);
        try {
            String res = jsonRun(user, exec, group, permissions);
            return JsonBuilder.create(res)
                .build();
        } catch (HcatException e) {
            throw new HcatException("unable to show databases for: " + 
                dbPattern, e.execBean);
        }
    }

    /**
     * Create a database with the given name
     */
    public String createDatabase(String user, String db,
                                DatabaseDesc desc,
                                String group, String permissions)
        throws HcatException, NotAuthorizedException, BusyException,
            ExecuteException, IOException
    {
        String exec = "create database if not exists " + db;
        if (desc != null) {
            if (desc.comment != null && !desc.comment.trim().equals("")) {
                exec += " comment '" + desc.comment + "'";
            }
            if (desc.location != null && !desc.location.trim().equals("")) {
                exec += " location '" + desc.location + "'";
            }
            if (desc.properties != null && !desc.properties.isEmpty()) {
                exec += " with dbproperties (";
                for (String key : desc.properties.keySet()) {
                    exec += "'" + key + "'='" + desc.properties.get(key) + "', ";
                }
                exec = exec.substring(0, exec.length() - 2);
                exec += ");";
            } else {
                exec += ";";
            }
        }
        String res = jsonRun(user, exec, group, permissions);
        return JsonBuilder.create()
            .put("database", db)
            .build();
    }

    /**
     * Drop the listed database
     */
    public String dropDatabase(String user, String db, String param,
                                String group, String permissions)
        throws HcatException, NotAuthorizedException, BusyException,
            ExecuteException, IOException
    {
        String exec = "drop database if exists " + db;
        if (param == null) {
            param = "";
        }
        if (param.toLowerCase().trim().equals("restrict")) {
            exec += " restrict;";
        } else if (param.toLowerCase().trim().equals("cascade")) {
            exec += " cascade;";
        } else {
            exec += ";";
        }
        String res = jsonRun(user, exec, group, permissions);
        return JsonBuilder.create()
                .put("database", db)
                .build();
    }

    /**
     * Create a table.
     */
    public String createTable(String user, String db,
                              TableDesc desc,
                              String group, String permissions)
        throws HcatException, NotAuthorizedException, BusyException,
        ExecuteException, IOException
    {
        throw new HcatException("not implemented", null);
    }

    /**
     * Return a json description of the table.
     */
    public String describeTable(String user, String db, String table, boolean extended,
                                String group, String permissions)
        throws HcatException, NotAuthorizedException, BusyException,
        ExecuteException, IOException
    {
        String exec = "use " + db + "; ";
        if (extended)
            exec += "desc extended " + table + "; ";
        else
            exec += "desc " + table + "; ";
        try {
            String res = jsonRun(user, exec, group, permissions);
            return JsonBuilder.create(res)
                .put("database", db)
                .put("table", table)
                .build();
        } catch (HcatException e) {
            throw new HcatException("unable to describe table: " + table, e.execBean);
        }
    }

    /**
     * Return a json "show table like".  This will return a list of
     * tables.
     */
    public String showTables(String user, String db, String tablePattern,
                             String group, String permissions)
        throws HcatException, NotAuthorizedException, BusyException,
        ExecuteException, IOException
    {
        String exec = String.format("use %s; show tables like '%s';",
                                    db, tablePattern);
        try {
            String res = jsonRun(user, exec, group, permissions);
            return JsonBuilder.create(res)
                .put("database", db)
                .build();
        } catch (HcatException e) {
            throw new HcatException("unable to show tables for: " + tablePattern,
                                    e.execBean);
        }
    }

    /**
     * Return a json "show table extended like".  This will return
     * only the first single table.
     */
    public String showExtendedTable(String user, String db, String table,
                                    String group, String permissions)
        throws HcatException, NotAuthorizedException, BusyException,
        ExecuteException, IOException
    {
        String exec = String.format("use %s; show table extended like %s;",
                                    db, table);
        try {
            String res = jsonRun(user, exec, group, permissions);
            return JsonBuilder.create(singleTable(res))
                .remove("tableName")
                .put("database", db)
                .put("table", table)
                .build();
        } catch (HcatException e) {
            throw new HcatException("unable to show table: " + table, e.execBean);
        }
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
     * Drop a table.
     */
    public String dropTable(String user, String db, String table,
                            String group, String permissions)
        throws HcatException, NotAuthorizedException, BusyException,
        ExecuteException, IOException
    {
        String exec = String.format("use %s; drop table %s;", db, table);
        try {
            jsonRun(user, exec, group, permissions, true);
            return JsonBuilder.create()
                .put("database", db)
                .put("table", table)
                .build();
        } catch (HcatException e) {
            throw new HcatException("unable to drop table: " + table, e.execBean);
        }
    }

    /**
     * Drop a table.
     */
    public String renameTable(String user, String db,
                              String oldTable, String newTable,
                              String group, String permissions)
        throws HcatException, NotAuthorizedException, BusyException,
        ExecuteException, IOException
    {
        String exec = String.format("use %s; alter table %s rename to %s;",
                                    db, oldTable, newTable);
        try {
            jsonRun(user, exec, group, permissions, true);
            return JsonBuilder.create()
                .put("database", db)
                .put("table", newTable)
                .build();
        } catch (HcatException e) {
            throw new HcatException("unable to rename table: " + oldTable,
                                    e.execBean);
        }
    }

    /**
     * Return a json description of the partitions.
     */
    public String showPartitions(String user, String db, String table,
                                 String group, String permissions)
        throws HcatException, NotAuthorizedException, BusyException,
        ExecuteException, IOException
    {
        String exec = "use " + db + "; ";
        exec += "show partitions " + table + "; ";
        try {
            String res = jsonRun(user, exec, group, permissions);
            return JsonBuilder.create(res)
                .put("database", db)
                .put("table", table)
                .build();
        } catch (HcatException e) {
            throw new HcatException("unable to show partitions for table: " + table,
                                    e.execBean);
        }
    }

    /**
     * Return a json description of one partition.
     */
    public String showOnePartition(String user, String db, String table, String partition,
                                   String group, String permissions)
        throws HcatException, NotAuthorizedException, BusyException,
        ExecuteException, IOException
    {
        String exec = "use " + db + "; ";
        exec += "show table extended like " + table
            + " partition (" + partition + "); ";
        try {
            String res = singleTable(jsonRun(user, exec, group, permissions));
            return JsonBuilder.create(res)
                .remove("tableName")
                .put("database", db)
                .put("table", table)
                .put("partition", partition)
                .build();
        } catch (HcatException e) {
            throw new HcatException("unable to show partition: "
                                    + table + " " + partition,
                                    e.execBean);
        }
    }

    /**
     * Add one partition.
     */
    public String addOnePartition(String user, String db, String table,
                                  PartitionDesc desc,
                                  String group, String permissions)
        throws HcatException, NotAuthorizedException, BusyException,
        ExecuteException, IOException
    {
        String exec = String.format("use %s; alter table %s add", db, table);
        if (desc.ifNotExists)
            exec += " if not exists";
        exec += String.format(" partition (%s)", desc.partition);
        if (TempletonUtils.isset(desc.location))
            exec += String.format(" location '%s'", desc.location);
        exec += ";";
        try {
            jsonRun(user, exec, group, permissions, true);

            return JsonBuilder.create()
                .put("database", db)
                .put("table", table)
                .put("partition", desc.partition)
                .build();
        } catch (HcatException e) {
            throw new HcatException("unable to add partition: " + desc,
                                    e.execBean);
        }
    }

    /**
     * Drop a partition.
     */
    public String dropPartition(String user, String db, String table, String partition,
                                String group, String permissions)
        throws HcatException, NotAuthorizedException, BusyException,
        ExecuteException, IOException
    {
        String exec
            = String.format("use %s; alter table %s drop if exists partition (%s)",
                            db, table, partition);
        try {
            jsonRun(user, exec, group, permissions, true);

            return JsonBuilder.create()
                .put("database", db)
                .put("table", table)
                .build();
        } catch (HcatException e) {
            throw new HcatException("unable to drop partition: " + partition,
                                    e.execBean);
        }
    }

    /**
     * Return a json description of the columns.  Same as
     * describeTable.
     */
    public String showColumns(String user, String db, String table,
                              String group, String permissions)
        throws HcatException, NotAuthorizedException, BusyException,
        ExecuteException, IOException
    {
        try {
            return describeTable(user, db, table, false, group, permissions);
        } catch (HcatException e) {
            throw new HcatException("unable to show columns for table: " + table,
                                    e.execBean);
        }
    }

    /**
     * Return a json description of one column.
     */
    public String showOneColumn(String user, String db, String table, String column,
                                String group, String permissions)
        throws SimpleWebException, NotAuthorizedException, BusyException,
        ExecuteException, IOException
    {
        String res = showColumns(user, db, table, group, permissions);
        final JsonBuilder builder = JsonBuilder.create(res);
        List<Map> cols = (List) (builder.getMap().get("columns"));

        Map found = null;
        for (Map col : cols) {
            if (column.equals(col.get("name"))) {
                found = col;
                break;
            }
        }

        if (found == null)
            throw new SimpleWebException(500, "unable to find column " + column,
                                         new HashMap<String, Object>() {{
                                                 put("description", builder.getMap());
                                             }});

        return builder
            .remove("columns")
            .put("column", found)
            .build();
    }

    /**
     * Add one column.
     */
    public String addOneColumn(String user, String db, String table,
                               ColumnDesc desc,
                               String group, String permissions)
        throws HcatException, NotAuthorizedException, BusyException,
        ExecuteException, IOException
    {
        String exec = String.format("use %s; alter table %s add columns (%s %s",
                                    db, table, desc.name, desc.type);
        if (TempletonUtils.isset(desc.comment))
            exec += String.format(" comment '%s'", desc.comment);
        exec += ");";
        try {
            jsonRun(user, exec, group, permissions, true);

            return JsonBuilder.create()
                .put("database", db)
                .put("table", table)
                .put("column", desc.name)
                .build();
        } catch (HcatException e) {
            throw new HcatException("unable to add column: " + desc,
                                    e.execBean);
        }
    }

    /**
     * Drop a column.
     */
    public String dropColumn(String user, String db, String table,
                             String column, String group, String permissions)
        throws HcatException, NotAuthorizedException, BusyException,
        ExecuteException, IOException
    {
        throw new HcatException("not implemented", null);
    }

    // Check that the hcat result is valid and error free
    private boolean isValid(ExecBean eb, boolean checkOutput) {
        if (eb == null)
            return false;
        if (eb.exitcode != 0)
            return false;
        if (checkOutput)
            if (TempletonUtils.isset(eb.stdout))
                return false;
        return true;
    }

    // Run an hcat expression and return just the json outout.
    private String jsonRun(String user, String exec,
                           String group, String permissions,
                           boolean checkOutput)
        throws HcatException, NotAuthorizedException, BusyException,
        ExecuteException, IOException
    {
        ExecBean res = run(user, exec, true, group, permissions);

        if (! isValid(res, checkOutput))
            throw new HcatException("Failure calling hcat: " + exec, res);

        return res.stdout;
    }

    // Run an hcat expression and return just the json outout.
    private String jsonRun(String user, String exec,
                           String group, String permissions)
        throws HcatException, NotAuthorizedException, BusyException,
        ExecuteException, IOException
    {
        return jsonRun(user, exec, group, permissions, false);
    }
}
