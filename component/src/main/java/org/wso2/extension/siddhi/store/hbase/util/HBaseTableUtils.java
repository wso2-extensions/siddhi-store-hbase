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

import io.siddhi.core.exception.OperationNotSupportedException;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.expression.condition.Compare;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.wso2.extension.siddhi.store.hbase.condition.BasicCompareOperation;
import org.wso2.extension.siddhi.store.hbase.condition.HBaseCompiledCondition;
import org.wso2.extension.siddhi.store.hbase.condition.Operand;
import org.wso2.extension.siddhi.store.hbase.condition.Operand.Constant;
import org.wso2.extension.siddhi.store.hbase.condition.Operand.StoreVariable;
import org.wso2.extension.siddhi.store.hbase.condition.Operand.StreamVariable;
import org.wso2.extension.siddhi.store.hbase.exception.HBaseTableException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.wso2.extension.siddhi.store.hbase.util.HBaseEventTableConstants.KEY_SEPARATOR;

/**
 * Class for holding various utility methods required by by the HBase table implementation.
 */
public class HBaseTableUtils {

    /**
     * Utility method which can be used to check if a given string instance is null or empty.
     *
     * @param field the string instance to be checked.
     * @return true if the field is null or empty.
     */
    public static boolean isEmpty(String field) {
        return (field == null || field.trim().length() == 0);
    }

    /**
     * Infer the ordinals of the attributes which are to be considered as primary keys. These keys will make up the
     * rowID of a particular record when written.
     *
     * @param schema      the schema defined for the table.
     * @param primaryKeys the store annotation for primary key fields.
     * @return a list of integers which comprise of the ordinals.
     */
    public static List<Integer> inferPrimaryKeyOrdinals(List<Attribute> schema, Annotation primaryKeys) {
        List<String> elements = schema.stream().map(Attribute::getName).map(String::toLowerCase)
                .collect(Collectors.toList());
        List<String> keys = Arrays.asList(primaryKeys.getElements().get(0).getValue().split(","));
        return keys.stream().map(String::trim).map(String::toLowerCase).map(candidateKey -> {
            int index = elements.indexOf(candidateKey);
            if (index == -1) {
                throw new HBaseTableException("Specified primary key '" + candidateKey + "' does not exist as " +
                        "part of the table schema. Please check your query and try again.");
            }
            return index;
        }).collect(Collectors.toList());
    }

    /**
     * Method for constructing a record (i.e. an array of objects) based on the result of an HBase table read operation.
     *
     * @param rowID        the HBase rowID of the record.
     * @param columnFamily the column family for which the record belongs to.
     * @param result       the raw result from the table read.
     * @param schema       the schema of the table.
     * @return the constructed record.
     */
    public static Object[] constructRecord(String rowID, String columnFamily, Result result, List<Attribute> schema) {
        List<byte[]> columns = new ArrayList<>();
        schema.forEach(attribute -> {
            Cell dataCell = result.getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes(attribute.getName()));
            if (dataCell == null) {
                throw new HBaseTableException("No data found for row '" + rowID + "'.");
            }
            columns.add(CellUtil.cloneValue(dataCell));
        });
        if (columns.size() != schema.size()) {
            throw new HBaseTableException("Data found on row '" + rowID + "' does not match the schema, and " +
                    "cannot be decoded.");
        }
        return columns.stream().map(column -> decodeCell(column, schema.get(columns.indexOf(column)).getType(), rowID))
                .toArray();
    }

    /**
     * Utility method for generating the HBase row key value from a given record.
     *
     * @param record      the record for which the key should be generated.
     * @param schema      the tables's schema, from which the fields' names and types are found.
     * @param keyOrdinals the ordinals of the primary keys in the schema.
     * @return the key in a string format.
     */
    public static String generatePrimaryKeyValue(Object[] record, List<Attribute> schema, List<Integer> keyOrdinals) {
        if (keyOrdinals.size() == 0) {
            return UUID.randomUUID().toString();
        }
        StringBuilder keyString = new StringBuilder();
        for (Integer key : keyOrdinals) {
            keyString.append(stringifyCell(schema.get(key).getType(), record[key]));
            if (key != keyOrdinals.size() - 1) {
                keyString.append(KEY_SEPARATOR);
            }
        }
        return keyString.toString();
    }

    /**
     * Infer the values of the rowID field based on the conditions and the values for the stream variables
     * in the conditions.
     *
     * @param parameterMaps     the condition paramater maps which contain the value of the stream variables in the
     *                          conditions, per record.
     * @param compiledCondition the compiled condition for the query.
     * @param primaryKeys       the table's primary key fields as a list of attributes.
     * @return a list of rowIDs.
     */
    public static List<String> getKeysForParameters(List<Map<String, Object>> parameterMaps,
                                                    HBaseCompiledCondition compiledCondition,
                                                    List<Attribute> primaryKeys) {
        return parameterMaps.stream().map(parameterMap ->
                inferKeyFromCondition(parameterMap, compiledCondition, primaryKeys)).collect(Collectors.toList());
    }

    /**
     * Infer the rowID to be used for a potential record in HBase based on the conditions given and the values for any
     * constants and stream variables.
     *
     * @param parameterMap      parameter map containing values for the stream variables.
     * @param compiledCondition the compiled condition.
     * @param primaryKeys       the primary keys defined in the table.
     * @return a string rowID
     */
    public static String inferKeyFromCondition(Map<String, Object> parameterMap,
                                               HBaseCompiledCondition compiledCondition, List<Attribute> primaryKeys) {
        StringBuilder keyString = new StringBuilder();
        List<BasicCompareOperation> operations = compiledCondition.getOperations();
        primaryKeys.forEach(key -> {
            operations.forEach(operation -> {
                Operand operand1 = operation.getOperand1();
                Operand operand2 = operation.getOperand2();
                if (operation.getOperator() == Compare.Operator.EQUAL) {
                    // Checking if one of the operands have the primary key as its name.
                    if (operand1 instanceof StoreVariable &&
                            (((StoreVariable) operand1).getName().equalsIgnoreCase(key.getName()))) {
                        // Is operand 1 a primary key?
                        if (operand2 instanceof StreamVariable) {
                            // If the other operand is a stream variable, get its value from the parameter map.
                            keyString.append(stringifyCell(key.getType(),
                                    parameterMap.get(((StreamVariable) operand2).getName())));
                        } else if (operand2 instanceof Constant) {
                            // Or if the other operand is a constant, directly add its value.
                            keyString.append(stringifyCell(key.getType(), ((Constant) operand2).getValue()));
                        }
                    } else if (operand2 instanceof StoreVariable &&
                            (((StoreVariable) operand2).getName().equalsIgnoreCase(key.getName()))) {
                        // Or is operand 2 a primary key?
                        if (operand1 instanceof StreamVariable) {
                            keyString.append(stringifyCell(key.getType(),
                                    parameterMap.get(((StreamVariable) operand1).getName())));
                        } else if (operand1 instanceof Constant) {
                            keyString.append(stringifyCell(key.getType(), ((Constant) operand1).getValue()));
                        }
                    }
                }
            });
            if (primaryKeys.indexOf(key) != primaryKeys.size() - 1) {
                keyString.append(KEY_SEPARATOR);
            }
        });
        return keyString.toString();
    }

    /**
     * Util method for converting a field in a given record to string form, to be used when constructing rowIDs.
     *
     * @param type  the type of the field.
     * @param value the field value.
     * @return a string representation of the field.
     */
    private static String stringifyCell(Attribute.Type type, Object value) {
        String output;
        switch (type) {
            case BOOL:
                output = Boolean.toString((boolean) value);
                break;
            case DOUBLE:
                output = Double.toString((double) value);
                break;
            case FLOAT:
                output = Float.toString((float) value);
                break;
            case INT:
                output = Integer.toString((int) value);
                break;
            case LONG:
                output = Long.toString((long) value);
                break;
            case STRING:
                output = (String) value;
                break;
            default:
                throw new OperationNotSupportedException("Unsupported column type found as primary key: " + type +
                        "Please check your query and try again.");
        }
        return output;
    }

    /**
     * Method for encoding a given field in a record for storage in HBase.
     *
     * @param type  the type of the field.
     * @param value the field's value.
     * @param row   the rowID of the record for which this field belongs to.
     * @return the encoded value of the field in byte[] form.
     */
    public static byte[] encodeCell(Attribute.Type type, Object value, String row) {
        byte[] output = null;
        switch (type) {
            case BOOL:
                output = Bytes.toBytes((boolean) value);
                break;
            case DOUBLE:
                output = Bytes.toBytes((double) value);
                break;
            case FLOAT:
                output = Bytes.toBytes((float) value);
                break;
            case INT:
                output = Bytes.toBytes((int) value);
                break;
            case LONG:
                output = Bytes.toBytes((long) value);
                break;
            case OBJECT:
                output = encodeBinaryData(value, row);
                break;
            case STRING:
                output = Bytes.toBytes((String) value);
                break;
        }
        return output;
    }

    /**
     * Method which is used for encoding binary data for storage in HBase.
     *
     * @param object the binary field.
     * @param row    the rowID for which this field belongs to.
     * @return the encoded value of the field in byte[] form.
     */
    private static byte[] encodeBinaryData(Object object, String row) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutput out = new ObjectOutputStream(bos)) {
            out.writeObject(object);
            return bos.toByteArray();
        } catch (IOException e) {
            if (row == null) {
                throw new HBaseTableException("Error encoding data : " + e.getMessage(), e);
            } else {
                throw new HBaseTableException("Error encoding data for row '" + row + "' : " + e.getMessage(), e);
            }
        }
    }

    /**
     * Method for decoding the value of a cell read from HBase.
     *
     * @param column the column which was read from HBase.
     * @param type   the type of the field (as given in the table's schema).
     * @param row    the rowID for which this field belongs to.
     * @return the decoded field value.
     */
    private static Object decodeCell(byte[] column, Attribute.Type type, String row) {
        Object output = null;
        switch (type) {
            case BOOL:
                output = Bytes.toBoolean(column);
                break;
            case DOUBLE:
                output = Bytes.toDouble(column);
                break;
            case FLOAT:
                output = Bytes.toFloat(column);
                break;
            case INT:
                output = Bytes.toInt(column);
                break;
            case LONG:
                output = Bytes.toLong(column);
                break;
            case OBJECT:
                output = decodeBinaryData(column, row);
                break;
            case STRING:
                output = Bytes.toString(column);
                break;
        }
        return output;
    }

    /**
     * Method which is used for decoding binary data read from HBase.
     *
     * @param bytes the value that was read.
     * @param row   the rowID for which this field belongs to.
     * @return the decoded field value.
     */
    private static Object decodeBinaryData(byte[] bytes, String row) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
             ObjectInput in = new ObjectInputStream(bis)) {
            return in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new HBaseTableException("Error converting data from row '" + row + "' : " + e.getMessage(), e);
        }
    }

    /**
     * Method for converting condition expressions to HBase Filter instances.
     *
     * @param operations   the list of compare operations in the condition.
     * @param parameters   the values for any stream variables specified in the condition.
     * @param columnFamily the column family for which the operations should be done.
     * @return a FilterList instance which contains all applicable filters.
     */
    public static FilterList convertConditionsToFilters(List<BasicCompareOperation> operations,
                                                        Map<String, Object> parameters, String columnFamily) {
        FilterList filterList = new FilterList();
        operations.stream()
                .map(operation -> initializeFilter(operation, parameters, columnFamily))
                .filter(Objects::nonNull)
                .forEach(filterList::addFilter);
        return filterList;
    }

    /**
     * Method which initializes a single filter for a given condition.
     *
     * @param operation    a particular compare operation.
     * @param parameters   the values for any stream variables specified in the operation.
     * @param columnFamily the column family for which the operation should be done.
     * @return an HBase Filter instance.
     */
    private static Filter initializeFilter(BasicCompareOperation operation, Map<String, Object> parameters,
                                           String columnFamily) {
        Operand operand1 = operation.getOperand1();
        Operand operand2 = operation.getOperand2();
        Filter filter;
        if (operand1 instanceof StoreVariable) {
            if (operand2 instanceof Constant) {
                return null;
            }
            byte[] conditionValue = encodeCell((operand2).getType(),
                    parameters.get(((StreamVariable) operand2).getName()), null);
            filter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily),
                    Bytes.toBytes(((StoreVariable) operand1).getName()),
                    convertOperator(operation.getOperator()), conditionValue);
        } else if (operand2 instanceof StoreVariable) {
            if (operand1 instanceof Constant) {
                return null;
            }
            byte[] conditionValue = encodeCell((operand1).getType(),
                    parameters.get(((StreamVariable) operand1).getName()), null);
            filter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily),
                    Bytes.toBytes(((StoreVariable) operand2).getName()),
                    convertOperator(operation.getOperator()), conditionValue);
        } else {
            throw new HBaseTableException("The HBase table implementation requires that either one of the operands " +
                    "used in a condition contain a table column reference. Please check your query and try again,");
        }
        return filter;
    }

    /**
     * Utility method for transforming operators from Siddhi syntax to HBase compare.
     *
     * @param operator the Siddhi compare operator.
     * @return an HBase compare operator.
     */
    public static CompareFilter.CompareOp convertOperator(Compare.Operator operator) {
        CompareFilter.CompareOp output = CompareFilter.CompareOp.NO_OP;
        switch (operator) {
            case LESS_THAN:
                output = CompareFilter.CompareOp.LESS;
                break;
            case LESS_THAN_EQUAL:
                output = CompareFilter.CompareOp.LESS_OR_EQUAL;
                break;
            case EQUAL:
                output = CompareFilter.CompareOp.EQUAL;
                break;
            case GREATER_THAN:
                output = CompareFilter.CompareOp.GREATER;
                break;
            case GREATER_THAN_EQUAL:
                output = CompareFilter.CompareOp.GREATER_OR_EQUAL;
                break;
            case NOT_EQUAL:
                output = CompareFilter.CompareOp.NOT_EQUAL;
                break;
        }
        return output;
    }

    /**
     * Method that can be used for quietly closing a closeable without having to worry about post-closure exceptions.
     *
     * @param closeable the closeable that should be closed quietly.
     */
    public static void closeQuietly(Closeable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (IOException ignore) {
            /* ignore */
        }
    }

}
