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
import org.apache.commons.lang.StringUtils;
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
    public String descDatabase(String user, String db, boolean extended)
        throws HcatException, NotAuthorizedException, BusyException,
        ExecuteException, IOException
    {
        String exec = "desc database " + db + "; ";
        if (extended) {
            exec = "desc database extended " + db + "; ";
        }
        String res = jsonRun(user, exec);
        return JsonBuilder.create(res)
            .put("database", db)
            .build();
    }

    /**
     * Return a json "show databases like".  This will return a list of
     * databases.
     */
    public String listDatabases(String user, String dbPattern)
        throws HcatException, NotAuthorizedException, BusyException,
        ExecuteException, IOException
    {
        String exec = String.format("show databases like '%s';",
                                    dbPattern);
        try {
            String res = jsonRun(user, exec);
            return JsonBuilder.create(res)
                .build();
        } catch (HcatException e) {
            throw new HcatException("unable to show databases for: " + dbPattern,
                                    e.execBean, exec);
        }
    }

    /**
     * Create a database with the given name
     */
    public String createDatabase(String user, DatabaseDesc desc)
        throws HcatException, NotAuthorizedException, BusyException,
        ExecuteException, IOException
    {
        String exec = "create database";
        if (desc.ifNotExists)
            exec += " if not exists";
        exec += " " + desc.database;
        if (TempletonUtils.isset(desc.comment))
            exec += String.format(" comment '%s'", desc.comment);
        if (TempletonUtils.isset(desc.location))
            exec += String.format(" location '%s'", desc.location);
        if (TempletonUtils.isset(desc.properties))
            exec += String.format(" with dbproperties (%s)",
                                  makePropertiesStatement(desc.properties));
        exec += ";";

        String res = jsonRun(user, exec, desc.group, desc.permissions);
        return JsonBuilder.create()
            .put("database", desc.database)
            .build();
    }

    /**
     * Drop the listed database
     */
    public String dropDatabase(String user, String db,
                               boolean ifExists, String option,
                               String group, String permissions)
        throws HcatException, NotAuthorizedException, BusyException,
        ExecuteException, IOException
    {
        String exec = "drop database";
        if (ifExists)
            exec += " if exists";
        exec += " " + db;
        if (TempletonUtils.isset(option))
            exec += " " + option;
        exec += ";";

        String res = jsonRun(user, exec, group, permissions);
        return JsonBuilder.create()
            .put("database", db)
            .build();
    }

    /**
     * Create a table.
     */
    public String createTable(String user, String db, TableDesc desc)
        throws HcatException, NotAuthorizedException, BusyException,
        ExecuteException, IOException
    {
        String exec = makeCreateTable(db, desc);

        try {
            jsonRun(user, exec, desc.group, desc.permissions, true);

            return JsonBuilder.create()
                .put("database", db)
                .put("table", desc.table)
                .build();
        } catch (final HcatException e) {
            throw new HcatException("unable to create table: " + desc.table,
                                    e.execBean, exec);
        }
    }

    /**
     * Return a json description of the table.
     */
    public String descTable(String user, String db, String table, boolean extended)
        throws HcatException, NotAuthorizedException, BusyException,
        ExecuteException, IOException
    {
        String exec = "use " + db + "; ";
        if (extended)
            exec += "desc extended " + table + "; ";
        else
            exec += "desc " + table + "; ";
        try {
            String res = jsonRun(user, exec);
            return JsonBuilder.create(res)
                .put("database", db)
                .put("table", table)
                .build();
        } catch (HcatException e) {
            throw new HcatException("unable to describe table: " + table,
                                    e.execBean, exec);
        }
    }

    /**
     * Return a json "show table like".  This will return a list of
     * tables.
     */
    public String listTables(String user, String db, String tablePattern)
        throws HcatException, NotAuthorizedException, BusyException,
        ExecuteException, IOException
    {
        String exec = String.format("use %s; show tables like '%s';",
                                    db, tablePattern);
        try {
            String res = jsonRun(user, exec);
            return JsonBuilder.create(res)
                .put("database", db)
                .build();
        } catch (HcatException e) {
            throw new HcatException("unable to show tables for: " + tablePattern,
                                    e.execBean, exec);
        }
    }

    /**
     * Return a json "show table extended like".  This will return
     * only the first single table.
     */
    public String descExtendedTable(String user, String db, String table)
        throws HcatException, NotAuthorizedException, BusyException,
        ExecuteException, IOException
    {
        String exec = String.format("use %s; show table extended like %s;",
                                    db, table);
        try {
            String res = jsonRun(user, exec);
            return JsonBuilder.create(singleTable(res))
                .remove("tableName")
                .put("database", db)
                .put("table", table)
                .build();
        } catch (HcatException e) {
            throw new HcatException("unable to show table: " + table, e.execBean, exec);
        }
    }

    // Format a list of Columns for a create statement
    private String makeCols(List<ColumnDesc> cols) {
        ArrayList<String> res = new ArrayList<String>();
        for (ColumnDesc col : cols)
            res.add(makeOneCol(col));
        return StringUtils.join(res, ", ");
    }

    // Format a Column for a create statement
    private String makeOneCol(ColumnDesc col) {
        String res = String.format("%s %s", col.name, col.type);
        if (TempletonUtils.isset(col.comment))
            res += String.format(" comment '%s'", col.comment);
        return res;
    }

    // Make a create table statement
    private String makeCreateTable(String db, TableDesc desc) {
        String exec = String.format("use %s; create", db);

        if (desc.external)
            exec += " external";
        exec += " table";
        if (desc.ifNotExists)
            exec += " if not exists";
        exec += " " + desc.table;

        if (TempletonUtils.isset(desc.columns))
            exec += String.format("(%s)", makeCols(desc.columns));
        if (TempletonUtils.isset(desc.comment))
            exec += String.format(" comment '%s'", desc.comment);
        if (TempletonUtils.isset(desc.partitionedBy))
            exec += String.format(" partitioned by (%s)", makeCols(desc.partitionedBy));
        if (desc.clusteredBy != null)
            exec += String.format(" clustered by %s", makeClusteredBy(desc.clusteredBy));
        if (desc.format != null)
            exec += " " + makeStorageFormat(desc.format);
        if (TempletonUtils.isset(desc.location))
            exec += String.format(" location '%s'", desc.location);
        exec += ";";

        return exec;
    }

    // Format a clustered by statement
    private String makeClusteredBy(TableDesc.ClusteredByDesc desc) {
        String res = String.format("(%s)", StringUtils.join(desc.columnNames, ", "));
        if (TempletonUtils.isset(desc.sortedBy))
            res += String.format(" sorted by %s", makeClusterSortList(desc.sortedBy));
        res += String.format(" into %s buckets", desc.numberOfBuckets);

        return res;
    }

    // Format a sorted by statement
    private String makeClusterSortList(List<TableDesc.ClusterSortOrderDesc> descs) {
        ArrayList<String> res = new ArrayList<String>();
        for (TableDesc.ClusterSortOrderDesc desc : descs)
            res.add(makeOneClusterSort(desc));
        return StringUtils.join(res, ", ");
    }

    // Format a single cluster sort statement
    private String makeOneClusterSort(TableDesc.ClusterSortOrderDesc desc) {
        return String.format("%s %s", desc.columnName, desc.order.toString());
    }

    // Format the storage format statements
    private String makeStorageFormat(TableDesc.StorageFormatDesc desc) {
        String res = "";

        if (desc.rowFormat != null)
            res += makeRowFormat(desc.rowFormat);
        if (TempletonUtils.isset(desc.storedAs))
            res += String.format(" stored as %s", desc.storedAs);
        if (desc.storedBy != null)
            res += " " + makeStoredBy(desc.storedBy);

        return res;
    }

    // Format the row format statement
    private String makeRowFormat(TableDesc.RowFormatDesc desc) {
        String res =
            makeTermBy(desc.fieldsTerminatedBy, "fields")
            + makeTermBy(desc.collectionItemsTerminatedBy, "collection items")
            + makeTermBy(desc.mapKeysTerminatedBy, "map keys")
            + makeTermBy(desc.linesTerminatedBy, "lines");

        if (TempletonUtils.isset(res))
            return "delimited" + res;
        else if (desc.serde != null)
            return makeSerdeFormat(desc.serde);
        else
            return "";
    }

    // A row format terminated by clause
    private String makeTermBy(char ch, String fieldName) {
        if (TempletonUtils.isset(ch))
            return String.format(" %s terminated by '%c'", fieldName, ch);
        else
            return "";
    }

    // Format the serde statement
    private String makeSerdeFormat(TableDesc.SerdeDesc desc) {
        String res = "serde " + desc.name;
        if (TempletonUtils.isset(desc.properties))
            res += String.format(" with serdeproperties (%s)",
                                 makePropertiesStatement(desc.properties));
        return res;
    }

    // Format the properties statement
    private String makePropertiesStatement(Map<String, String> properties) {
        ArrayList<String> res = new ArrayList<String>();
        for (Map.Entry<String, String> e : properties.entrySet())
            res.add(String.format("'%s'='%s'", e.getKey(), e.getValue()));
        return StringUtils.join(res, ", ");
    }

    // Format the stored by statement
    private String makeStoredBy(TableDesc.StoredByDesc desc) {
        String res = String.format("stored by '%s'", desc.className);
        if (TempletonUtils.isset(desc.properties))
            res += String.format(" with serdeproperties (%s)",
                                 makePropertiesStatement(desc.properties));
        return res;
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
    public String dropTable(String user, String db,
                            String table, boolean ifExists,
                            String group, String permissions)
        throws HcatException, NotAuthorizedException, BusyException,
        ExecuteException, IOException
    {
        String exec = String.format("use %s; drop table", db);
        if (ifExists)
            exec += " if exists";
        exec += String.format(" %s;", table);

        try {
            jsonRun(user, exec, group, permissions, true);
            return JsonBuilder.create()
                .put("database", db)
                .put("table", table)
                .build();
        } catch (HcatException e) {
            throw new HcatException("unable to drop table: " + table, e.execBean, exec);
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
                                    e.execBean, exec);
        }
    }

    /**
     * Return a json description of the partitions.
     */
    public String listPartitions(String user, String db, String table)
        throws HcatException, NotAuthorizedException, BusyException,
        ExecuteException, IOException
    {
        String exec = "use " + db + "; ";
        exec += "show partitions " + table + "; ";
        try {
            String res = jsonRun(user, exec);
            return JsonBuilder.create(res)
                .put("database", db)
                .put("table", table)
                .build();
        } catch (HcatException e) {
            throw new HcatException("unable to show partitions for table: " + table,
                                    e.execBean, exec);
        }
    }

    /**
     * Return a json description of one partition.
     */
    public String descOnePartition(String user, String db, String table,
                                   String partition)
        throws HcatException, NotAuthorizedException, BusyException,
        ExecuteException, IOException
    {
        String exec = "use " + db + "; ";
        exec += "show table extended like " + table
            + " partition (" + partition + "); ";
        try {
            String res = singleTable(jsonRun(user, exec));
            return JsonBuilder.create(res)
                .remove("tableName")
                .put("database", db)
                .put("table", table)
                .put("partition", partition)
                .build();
        } catch (HcatException e) {
            throw new HcatException("unable to show partition: "
                                    + table + " " + partition,
                                    e.execBean,
                                    exec);
        }
    }

    /**
     * Add one partition.
     */
    public String addOnePartition(String user, String db, String table,
                                  PartitionDesc desc)
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
            jsonRun(user, exec, desc.group, desc.permissions, true);

            return JsonBuilder.create()
                .put("database", db)
                .put("table", table)
                .put("partition", desc.partition)
                .build();
        } catch (HcatException e) {
            throw new HcatException("unable to add partition: " + desc,
                                    e.execBean, exec);
        }
    }

    /**
     * Drop a partition.
     */
    public String dropPartition(String user, String db,
                                String table, String partition, boolean ifExists,
                                String group, String permissions)
        throws HcatException, NotAuthorizedException, BusyException,
        ExecuteException, IOException
    {
        String exec = String.format("use %s; alter table %s drop", db, table);
        if (ifExists)
            exec += " if exists";
        exec += String.format(" partition (%s);", partition);

        try {
            jsonRun(user, exec, group, permissions, true);

            return JsonBuilder.create()
                .put("database", db)
                .put("table", table)
                .build();
        } catch (HcatException e) {
            throw new HcatException("unable to drop partition: " + partition,
                                    e.execBean, exec);
        }
    }

    /**
     * Return a json description of the columns.  Same as
     * describeTable.
     */
    public String listColumns(String user, String db, String table)
        throws HcatException, NotAuthorizedException, BusyException,
        ExecuteException, IOException
    {
        try {
            return descTable(user, db, table, false);
        } catch (HcatException e) {
            throw new HcatException("unable to show columns for table: " + table,
                                    e.execBean, e.statement);
        }
    }

    /**
     * Return a json description of one column.
     */
    public String descOneColumn(String user, String db, String table, String column)
        throws SimpleWebException, NotAuthorizedException, BusyException,
        ExecuteException, IOException
    {
        String res = listColumns(user, db, table);
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
                               ColumnDesc desc)
        throws HcatException, NotAuthorizedException, BusyException,
        ExecuteException, IOException
    {
        String exec = String.format("use %s; alter table %s add columns (%s %s",
                                    db, table, desc.name, desc.type);
        if (TempletonUtils.isset(desc.comment))
            exec += String.format(" comment '%s'", desc.comment);
        exec += ");";
        try {
            jsonRun(user, exec, desc.group, desc.permissions, true);

            return JsonBuilder.create()
                .put("database", db)
                .put("table", table)
                .put("column", desc.name)
                .build();
        } catch (HcatException e) {
            throw new HcatException("unable to add column: " + desc,
                                    e.execBean, exec);
        }
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
            throw new HcatException("Failure calling hcat: " + exec, res, exec);

        return res.stdout;
    }

    // Run an hcat expression and return just the json outout.  No
    // permissions set.
    private String jsonRun(String user, String exec)
        throws HcatException, NotAuthorizedException, BusyException,
        ExecuteException, IOException
    {
        return jsonRun(user, exec, null, null);
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
