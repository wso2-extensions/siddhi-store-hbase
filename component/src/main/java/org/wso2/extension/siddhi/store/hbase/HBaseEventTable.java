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
package org.wso2.extension.siddhi.store.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.wso2.extension.siddhi.store.hbase.condition.HBaseCompiledCondition;
import org.wso2.extension.siddhi.store.hbase.exception.HBaseTableException;
import org.wso2.extension.siddhi.store.hbase.iterator.HBaseGetIterator;
import org.wso2.extension.siddhi.store.hbase.iterator.HBaseScanIterator;
import org.wso2.extension.siddhi.store.hbase.util.HBaseTableUtils;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.table.record.AbstractRecordTable;
import org.wso2.siddhi.core.table.record.ExpressionBuilder;
import org.wso2.siddhi.core.table.record.RecordIterator;
import org.wso2.siddhi.core.util.SiddhiConstants;
import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;
import org.wso2.siddhi.core.util.collection.operator.CompiledExpression;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.annotation.Annotation;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.TableDefinition;
import org.wso2.siddhi.query.api.util.AnnotationHelper;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.wso2.extension.siddhi.store.hbase.util.HBaseEventTableConstants.ANNOTATION_ELEMENT_CF_NAME;
import static org.wso2.extension.siddhi.store.hbase.util.HBaseEventTableConstants.ANNOTATION_ELEMENT_TABLE_NAME;
import static org.wso2.extension.siddhi.store.hbase.util.HBaseEventTableConstants.DEFAULT_CF_NAME;
import static org.wso2.extension.siddhi.store.hbase.util.HBaseEventTableConstants.HBASE_BATCH_SIZE;
import static org.wso2.siddhi.core.util.SiddhiConstants.ANNOTATION_STORE;

public class HBaseEventTable extends AbstractRecordTable {

    private static final Log log = LogFactory.getLog(HBaseEventTable.class);

    private ConfigReader configReader;
    private Connection connection;
    private List<Attribute> schema;
    private Annotation storeAnnotation;
    private Integer[] primaryKeyOrdinals;
    private String tableName;
    private String columnFamily;
    private boolean noKeys;
    private int batchSize;

    @Override
    protected void init(TableDefinition tableDefinition, ConfigReader configReader) {
        this.schema = tableDefinition.getAttributeList();
        this.storeAnnotation = AnnotationHelper.getAnnotation(ANNOTATION_STORE, tableDefinition.getAnnotations());
        Annotation primaryKeys = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_PRIMARY_KEY,
                tableDefinition.getAnnotations());
        String tableName = storeAnnotation.getElement(ANNOTATION_ELEMENT_TABLE_NAME);
        String cfName = storeAnnotation.getElement(ANNOTATION_ELEMENT_CF_NAME);
        this.tableName = HBaseTableUtils.isEmpty(tableName) ? tableDefinition.getId() : tableName;
        this.columnFamily = HBaseTableUtils.isEmpty(cfName) ? DEFAULT_CF_NAME : cfName;
        this.noKeys = primaryKeys == null;
        this.primaryKeyOrdinals = HBaseTableUtils.inferPrimaryKeyOrdinals(schema, primaryKeys);
        if (configReader != null) {
            this.configReader = configReader;
        } else {
            this.configReader = new BasicConfigReader();
        }
    }

    @Override
    protected void add(List<Object[]> records) throws ConnectionUnavailableException {
        records.forEach(this::insertRecord);
    }

    @Override
    protected RecordIterator<Object[]> find(Map<String, Object> findConditionParameterMap,
                                            CompiledCondition compiledCondition)
            throws ConnectionUnavailableException {
        if (noKeys) {
            return new HBaseScanIterator(this.tableName, this.columnFamily, (HBaseCompiledCondition) compiledCondition,
                    this.connection, this.schema);
        } else {
            return new HBaseGetIterator();
        }
    }

    @Override
    protected boolean contains(Map<String, Object> map, CompiledCondition compiledCondition)
            throws ConnectionUnavailableException {
        if (noKeys) {
            return new HBaseScanIterator(this.tableName, this.columnFamily, (HBaseCompiledCondition) compiledCondition,
                    this.connection, this.schema).hasNext();
        } else {
            return new HBaseGetIterator().hasNext();
        }
    }

    @Override
    protected void delete(List<Map<String, Object>> list, CompiledCondition compiledCondition)
            throws ConnectionUnavailableException {

    }

    @Override
    protected void update(CompiledCondition compiledCondition, List<Map<String, Object>>
            list, Map<String, CompiledExpression> map, List<Map<String, Object>> list1)
            throws ConnectionUnavailableException {
        throw new OperationNotSupportedException("Record update operations are not supported by the HBase Table " +
                "implementation. Please check your query and try again");
    }

    @Override
    protected void updateOrAdd(CompiledCondition compiledCondition, List<Map<String, Object>> list,
                               Map<String, CompiledExpression> map, List<Map<String, Object>> list1,
                               List<Object[]> list2) throws ConnectionUnavailableException {

    }

    @Override
    protected CompiledCondition compileCondition(ExpressionBuilder expressionBuilder) {
        return null;
    }

    @Override
    protected CompiledExpression compileSetAttribute(ExpressionBuilder expressionBuilder) {
        return null;
    }

    @Override
    protected void connect() throws ConnectionUnavailableException {
        Configuration config = HBaseConfiguration.create();
        storeAnnotation.getElements().stream()
                .filter(Objects::nonNull)
                .filter(element -> !(element.getKey().equals(ANNOTATION_ELEMENT_TABLE_NAME)
                        || element.getKey().equals(ANNOTATION_ELEMENT_CF_NAME)))
                .filter(element -> !(HBaseTableUtils.isEmpty(element.getKey())
                        || HBaseTableUtils.isEmpty(element.getValue())))
                .forEach(element -> config.set(element.getKey(), element.getValue()));
        try {
            this.connection = ConnectionFactory.createConnection(config);
        } catch (IOException e) {
            throw new ConnectionUnavailableException("Failed to initialize store for table name '" +
                    this.tableName + "': " + e.getMessage(), e);
        }
        this.batchSize = Integer.parseInt(this.configReader.readConfig(HBASE_BATCH_SIZE, "5000"));
        this.checkAndCreateTable();
    }

    @Override
    protected void disconnect() {
        if (this.connection != null && !this.connection.isClosed()) {
            HBaseTableUtils.closeQuietly(this.connection);
        }
    }

    @Override
    protected void destroy() {
        this.disconnect();
        if (log.isDebugEnabled()) {
            log.debug("Destroyed HBase connection for store table " + this.tableName);
        }
    }

    private void checkAndCreateTable() {
        TableName table = TableName.valueOf(this.tableName);
        HTableDescriptor descriptor = new HTableDescriptor(table).addFamily(
                new HColumnDescriptor(this.columnFamily).setMaxVersions(1));
        Admin admin = null;
        try {
            admin = this.connection.getAdmin();
            if (admin.tableExists(table)) {
                log.debug("Table " + tableName + " already exists.");
                return;
            }
            admin.createTable(descriptor);
            log.debug("Table " + tableName + " created.");
        } catch (IOException e) {
            throw new HBaseTableException("Error creating table " + tableName + " : " + e.getMessage(), e);
        } finally {
            HBaseTableUtils.closeQuietly(admin);
        }
    }

    /**
     * Method which will perform an insertion operation for a given record, without updating the record's values
     * if it already exists.
     * Note that this method has to do an RPC call per record due to HBase API limitations. Hence, it is not recommended for high
     * throughput operations.
     *
     * @param record the record to be inserted into the HBase cluster.
     */
    private void insertRecord(Object[] record) {
        String rowID = HBaseTableUtils.generatePrimaryKeyValue(record, this.schema, this.primaryKeyOrdinals);
        Put put = new Put(Bytes.toBytes(rowID));
        byte[] firstColumn = Bytes.toBytes(this.schema.get(0).getName());
        for (int i = 0; i < this.schema.size(); i++) {
            Attribute column = this.schema.get(i);
            //method: CF, qualifier, value.
            put.addColumn(Bytes.toBytes(this.columnFamily), Bytes.toBytes(column.getName()),
                    HBaseTableUtils.encodeCell(column.getType(), record[i], rowID));
        }
        try (Table table = this.connection.getTable(TableName.valueOf(this.tableName))) {
            //method: rowID, CF, qualifier, value, Put.
            table.checkAndPut(Bytes.toBytes(rowID), Bytes.toBytes(this.columnFamily), firstColumn, null, put);
        } catch (IOException e) {
            if (log.isDebugEnabled()) {
                log.debug("Error while performing insert operation on table '" + this.tableName + "' on row '"
                        + rowID + "' :" + e.getMessage());
            }
            throw new HBaseTableException("Error while performing insert operation on table '" + this.tableName + "': "
                    + e.getMessage(), e);
        }
    }

    private static class BasicConfigReader implements ConfigReader {
        @Override
        public String readConfig(String name, String defaultValue) {
            return defaultValue;
        }

        @Override
        public Map<String, String> getAllConfigs() {
            return new HashMap<>();
        }
    }

}
