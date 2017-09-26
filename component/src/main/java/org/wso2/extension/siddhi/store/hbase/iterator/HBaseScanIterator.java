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
package org.wso2.extension.siddhi.store.hbase.iterator;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.wso2.extension.siddhi.store.hbase.condition.BasicCompareOperation;
import org.wso2.extension.siddhi.store.hbase.condition.HBaseCompiledCondition;
import org.wso2.extension.siddhi.store.hbase.exception.HBaseTableException;
import org.wso2.extension.siddhi.store.hbase.util.HBaseTableUtils;
import org.wso2.siddhi.core.table.record.RecordIterator;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class HBaseScanIterator implements RecordIterator<Object[]> {

    private Iterator<Result> resultIterator = Collections.emptyIterator();
    private String tableName;
    private String columnFamily;
    private List<Attribute> schema;
    private Table table;
    private Map<String, Object> parameters;

    public HBaseScanIterator(Map<String, Object> findConditionParameterMap, String tableName, String columnFamily,
                             HBaseCompiledCondition compiledCondition, Connection connection, List<Attribute> schema) {
        this.columnFamily = columnFamily;
        this.parameters = findConditionParameterMap;
        this.tableName = tableName;
        this.schema = schema;
        List<BasicCompareOperation> conditions = compiledCondition.getOperations();
        TableName finalName = TableName.valueOf(tableName);
        try {
            this.table = connection.getTable(finalName);
        } catch (IOException e) {
            throw new HBaseTableException("The table '" + tableName + "' could not be initialized for reading: "
                    + e.getMessage(), e);
        }
        Scan scan = new Scan()
                .addFamily(Bytes.toBytes(columnFamily))
                .setFilter(HBaseTableUtils.convertConditionsToFilters(conditions, parameters, this.columnFamily));
        try {
            ResultScanner scanner = table.getScanner(scan);
            this.resultIterator = scanner.iterator();
        } catch (IOException e) {
            throw new HBaseTableException("Error while reading records from table '" + tableName + "': "
                    + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws IOException {
        this.cleanup();
    }

    @Override
    public boolean hasNext() {
        return this.resultIterator.hasNext();
    }

    @Override
    public Object[] next() {
        if (!this.hasNext()) {
            this.cleanup();
        }
        Result currentResult = this.resultIterator.next();
        byte[] rowId = currentResult.getRow();
        Object[] record = HBaseTableUtils.constructRecord(Bytes.toString(rowId), this.columnFamily, currentResult, this.schema);
        if (record != null && record.length > 0) {
            return record;
        } else {
            throw new HBaseTableException("Invalid data found on row '" + Bytes.toString(rowId) + "' on table '" + this.tableName + "'.");
        }
    }

    @Override
    public void remove() {
        //Nothing to do since this is a read-only iterator.
    }

    private void cleanup() {
        HBaseTableUtils.closeQuietly(this.table);
    }
}
