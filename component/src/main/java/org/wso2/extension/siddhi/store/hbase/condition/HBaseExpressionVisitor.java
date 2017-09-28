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
package org.wso2.extension.siddhi.store.hbase.condition;

import org.wso2.extension.siddhi.store.hbase.exception.HBaseTableException;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.table.record.BaseExpressionVisitor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.expression.condition.Compare;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Class which is used by the Siddhi runtime for instructions on converting the SiddhiQL condition to the condition
 * format understood by the underlying HBase data store.
 */
public class HBaseExpressionVisitor extends BaseExpressionVisitor {

    private boolean readOnlyCondition;
    private boolean allKeyEquals;
    private boolean atomicCondition;
    private List<BasicCompareOperation> compareOps;
    private List<String> primaryKeys;

    private volatile BasicCompareOperation currentOperation;

    public HBaseExpressionVisitor(List<Attribute> primaryKeys) {
        this.primaryKeys = primaryKeys.stream().map(Attribute::getName).map(String::toLowerCase)
                .collect(Collectors.toList());
        this.compareOps = new ArrayList<>();
    }

    /**
     * Check if the condition specified contains any operations other than EQUALS. In this case, such conditions can be
     * used only in reading operations (i.e. find()).
     *
     * @return boolean true/false.
     */
    public boolean isReadOnlyCondition() {
        return readOnlyCondition;
    }

    /**
     * Check if all primary key fields are present in the condition in EQUALS form. If yes, then the rowID can be
     * inferred from the values in the condition.
     *
     * @return boolean true/false
     */
    public boolean isAllKeyEquals() {
        return allKeyEquals;
    }

    /**
     * Return the conditions composed by the visitor
     *
     * @return the compiled compare operations.
     */
    public List<BasicCompareOperation> getConditions() {
        this.preProcessConditions();
        return compareOps;
    }

    /**
     * Pre-process given conditions and check if they fall within the compatibility criteria imposed by the HBase API.
     * This involves checking for variable types as well as the boolean conditions above.
     */
    private void preProcessConditions() {
        List<String> equalsWithStoreVariables = new ArrayList<>();
        for (BasicCompareOperation operation : this.compareOps) {
            // Check if both operands are store variables, which may not be allowed.
            if (operation.getOperand1() instanceof Operand.StoreVariable &&
                    operation.getOperand2() instanceof Operand.StoreVariable) {
                throw new OperationNotSupportedException("The HBase Table implementation does not support " +
                        "conditions which have both operands as table columns. Please check your query and try again.");
            }
            // Construct a list of conditions which contain a store variable and an EQUALS condition. Will be used
            // later to check if all primary keys are present with an EQUALS condition.
            if (operation.getOperand1() instanceof Operand.StoreVariable) {
                if (operation.getOperator() == Compare.Operator.EQUAL) {
                    equalsWithStoreVariables.add(
                            ((Operand.StoreVariable) operation.getOperand1()).getName().toLowerCase(Locale.ROOT));
                }
            } else if (operation.getOperand2() instanceof Operand.StoreVariable) {
                if (operation.getOperator() == Compare.Operator.EQUAL) {
                    equalsWithStoreVariables.add(
                            ((Operand.StoreVariable) operation.getOperand2()).getName().toLowerCase(Locale.ROOT));
                }
            } else {
                throw new OperationNotSupportedException("The HBase Table implementation does not support " +
                        "conditions which do not contain at lease one reference to a table column. Please check your " +
                        "query and try again.");
            }
        }
        if (equalsWithStoreVariables.containsAll(primaryKeys)) {
            this.allKeyEquals = true;
        }
        // Check if the only condition passed by Siddhi to this level is boolean TRUE (i.e. there are no conditions).
        // In this case, all other conditions should be ignored.
        if (this.atomicCondition) {
            this.compareOps.clear();
            this.allKeyEquals = false;
        }
    }

    public void beginVisitCompare(Compare.Operator operator) {
        if (operator != Compare.Operator.EQUAL) {
            this.readOnlyCondition = true;
        }
        this.currentOperation = new BasicCompareOperation();
    }

    public void endVisitCompare(Compare.Operator operator) {
        this.currentOperation.setOperator(operator);
        if (this.currentOperation.isInvalid()) {
            throw new HBaseTableException("Error parsing condition operation: one or more of the condition " +
                    "parameters are invalid.");
        } else {
            this.compareOps.add(this.currentOperation);
        }
        this.currentOperation = null;
    }

    public void beginVisitCompareLeftOperand(Compare.Operator operator) {
        if (this.currentOperation.getOperand1() != null) {
            throw new HBaseTableException("Error parsing condition operation: one or more of the condition " +
                    "parameters are invalid.");
        }
    }

    public void endVisitCompareLeftOperand(Compare.Operator operator) {
        if (this.currentOperation.getOperand1() == null) {
            throw new HBaseTableException("Error parsing condition operation: one or more of the condition " +
                    "parameters are invalid.");
        }
    }

    public void beginVisitCompareRightOperand(Compare.Operator operator) {
        if (this.currentOperation.getOperand2() != null) {
            throw new HBaseTableException("Error parsing condition operation: one or more of the condition " +
                    "parameters are invalid.");
        }
    }

    public void endVisitCompareRightOperand(Compare.Operator operator) {
        if (this.currentOperation.getOperand2() == null) {
            throw new HBaseTableException("Error parsing condition operation: one or more of the condition " +
                    "parameters are invalid.");
        }
    }

    public void beginVisitConstant(Object value, Attribute.Type type) {
    }

    public void endVisitConstant(Object value, Attribute.Type type) {
        if (this.currentOperation == null && type == Attribute.Type.BOOL && value.equals(Boolean.TRUE)) {
            // Siddhi returns "true" when there are no conditions. Remove any and all conditions if this is the case.
            // This removal operation will be done during pre-processing the condition prior to compilation end.
            this.atomicCondition = true;
            return;
        }
        Operand.Constant constant = new Operand.Constant(value, type);
        if (this.currentOperation.getOperand1() == null) {
            this.currentOperation.setOperand1(constant);
        } else {
            this.currentOperation.setOperand2(constant);
        }
    }

    public void beginVisitStreamVariable(String id, String streamId, String attributeName, Attribute.Type type) {
    }

    public void endVisitStreamVariable(String id, String streamId, String attributeName, Attribute.Type type) {
        Operand.StreamVariable variable = new Operand.StreamVariable(id, type);
        if (this.currentOperation.getOperand1() == null) {
            this.currentOperation.setOperand1(variable);
        } else {
            this.currentOperation.setOperand2(variable);
        }
    }

    public void beginVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {
    }

    public void endVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {
        Operand.StoreVariable variable = new Operand.StoreVariable(attributeName, type);
        if (this.currentOperation.getOperand1() == null) {
            this.currentOperation.setOperand1(variable);
        } else {
            this.currentOperation.setOperand2(variable);
        }
    }

    public void beginVisitOr() {
        throw new OperationNotSupportedException("OR operations are not supported by the HBase Table " +
                "extension. Please check your query and try again");
    }

    public void beginVisitNot() {
        throw new OperationNotSupportedException("NOT operations are not supported by the HBase Table " +
                "extension. Please check your query and try again");
    }

    public void beginVisitIsNull(String streamId) {
        throw new OperationNotSupportedException("IS NULL operations are not supported by the HBase Table " +
                "extension. Please check your query and try again");
    }

    public void beginVisitIn(String storeId) {
        throw new OperationNotSupportedException("IN operations are not supported by the HBase Table " +
                "extension. Please check your query and try again");
    }

    public void beginVisitMath(MathOperator mathOperator) {
        throw new OperationNotSupportedException("Math operations are not supported by the HBase Table " +
                "extension. Please check your query and try again");
    }

    public void beginVisitAttributeFunction(String namespace, String functionName) {
        throw new OperationNotSupportedException("Function evaluations are not supported by the HBase Table " +
                "extension. Please check your query and try again");
    }

    public void beginVisitParameterAttributeFunction(int index) {
        throw new OperationNotSupportedException("Function evaluations are not supported by the HBase Table " +
                "extension. Please check your query and try again");
    }

}
