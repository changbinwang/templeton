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
import javax.xml.bind.annotation.XmlRootElement;

/**
 * A description of the table to create.
 */
@XmlRootElement
public class TableDesc {
    public boolean ifNotExists = false;
    public boolean external = false;
    public String table;
    public List<ColumnDesc> columns;
    public String comment;
    public List<ColumnDesc> partitions;

    /**
     * Create a new PartitionDesc
     */
    public TableDesc() {}

    public String toString() {
        return String.format("TableDesc(table=%s)",
                             table);
    }
}
