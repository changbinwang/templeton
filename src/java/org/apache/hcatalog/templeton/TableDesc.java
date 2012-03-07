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

import java.util.List;
import java.util.Map;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * A description of the table to create.
 */
@XmlRootElement
public class TableDesc extends GroupPermissionsDesc {
    public boolean ifNotExists = false;
    public boolean external = false;
    public String table;
    public String comment;
    public List<ColumnDesc> columns;
    public List<ColumnDesc> partitionedBy;
    public StorageFormatDesc format;
    public String location;
    public Map<String, String> tableProperties;

    /**
     * Create a new TableDesc
     */
    public TableDesc() {}

    public String toString() {
        return String.format("TableDesc(table=%s, columns=%s)", table, columns);
    }

    /**
     * The storage format.
     */
    @XmlRootElement
    public static class StorageFormatDesc {
        public RowFormatDesc rowFormat;
        public String storedAs;
        public StoredByDesc storedBy;

        public StorageFormatDesc() {}
    }

    /**
     * The Row Format.
     */
    @XmlRootElement
    public static class RowFormatDesc {
        public char fieldsTerminatedBy;
        public char collectionItemsTerminatedBy;
        public char mapKeysTerminatedBy;
        public char linesTerminatedBy;
        public SerdeDesc serde;

        public RowFormatDesc() {}
    }

    /**
     * The SERDE Row Format.
     */
    @XmlRootElement
    public static class SerdeDesc {
        public String name;
        public Map<String, String> properties;

        public SerdeDesc() {}
    }

    /**
     * How to store the table.
     */
    @XmlRootElement
    public static class StoredByDesc {
        public String className;
        public Map<String, String> properties;

        public StoredByDesc() {}
    }

}
