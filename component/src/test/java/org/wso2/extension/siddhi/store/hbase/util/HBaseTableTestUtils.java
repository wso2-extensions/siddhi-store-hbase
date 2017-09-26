/*
*  Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.wso2.extension.siddhi.store.hbase.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.wso2.extension.siddhi.store.hbase.exception.HBaseTableException;

import java.io.IOException;

public class HBaseTableTestUtils {

    private static Connection connection;

    public static final String TABLE_NAME = "HBaseTestTable";
    public static final String COLUMN_FAMILY = "AnalyticsFamily";
    public static final String ZK_QUORUM = "localhost";
    public static final int ZK_CLIENT_PORT = 2181;

    private static final Log log = LogFactory.getLog(HBaseTableTestUtils.class);

    private static Connection getConnection() {
        if (connection == null) {
            try {
                connection = ConnectionFactory.createConnection();
            } catch (IOException e) {
                throw new HBaseTableException("Cannot create connection due to: " + e.getMessage(), e);
            }
        }
        return connection;
    }

    public static synchronized void initializeTable(String tableName, String columnFamily) {
        TableName table = TableName.valueOf(tableName);
        HTableDescriptor descriptor = new HTableDescriptor(table).addFamily(
                new HColumnDescriptor(columnFamily).setMaxVersions(1));
        try (Admin admin = getConnection().getAdmin()) {
            if (admin.tableExists(table)) {
                admin.disableTable(table);
                admin.deleteTable(table);
            }
            admin.createTable(descriptor);
            log.debug("Table " + tableName + " created.");
        } catch (IOException e) {
            throw new HBaseTableException("Error creating table " + tableName + " : " + e.getMessage(), e);
        }
    }

    public static synchronized void dropTable(String tableName) {
        TableName table = TableName.valueOf(tableName);
        try (Admin admin = getConnection().getAdmin()) {
            if (!admin.tableExists(table)) {
                return;
            }
            admin.disableTable(table);
            admin.deleteTable(table);
            log.debug("Table " + tableName + " created.");
        } catch (IOException e) {
            throw new HBaseTableException("Error creating table " + tableName + " : " + e.getMessage(), e);
        }
    }

    public static int getRowsInTable(String tableName, String columnFamily) {
        int counter = 0;
        try (Table table = getConnection().getTable(TableName.valueOf(tableName))) {
            FilterList allFilters = new FilterList();
            allFilters.addFilter(new FirstKeyOnlyFilter());
            allFilters.addFilter(new KeyOnlyFilter());
            Scan scan = new Scan().addFamily(Bytes.toBytes(columnFamily)).setFilter(allFilters);
            try (ResultScanner sc = table.getScanner(scan)) {
                while (sc.iterator().hasNext()) {
                    counter++;
                }
            }
            return counter;
        } catch (IOException e) {
            throw new HBaseTableException("Error getting record count from table " + tableName + " : "
                    + e.getMessage(), e);
        }
    }
}
