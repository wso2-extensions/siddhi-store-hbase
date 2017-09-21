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

import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.table.record.BaseExpressionVisitor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.expression.condition.Compare;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class HBaseExpressionVisitor extends BaseExpressionVisitor {

    private boolean readOnlyCondition;
    private boolean allKeyEquals;
    private List<BasicCompareOperation> compareOps;
    private List<String> primaryKeys;

    private BasicCompareOperation currentOperation;

    public HBaseExpressionVisitor(List<Attribute> primaryKeys) {
        this.primaryKeys = primaryKeys.stream().map(Attribute::getName).map(String::toLowerCase)
                .collect(Collectors.toList());
        this.compareOps = new ArrayList<>();
    }

    public boolean isReadOnlyCondition() {
        return readOnlyCondition;
    }

    public boolean isAllKeyEquals() {
        return allKeyEquals;
    }

    public List<BasicCompareOperation> getConditions() {
        this.preProcessConditions();
        return compareOps;
    }

    private void preProcessConditions() {

        this.compareOps.forEach(operation -> {
            Operand op1 = operation.getOperand1();
            Operand op2 = operation.getOperand2();
            boolean isOp1Store = op1 instanceof Operand.StoreVariable;
            boolean isOp2Store = op2 instanceof Operand.StoreVariable;
            if (!isOp1Store && !isOp2Store) {
                throw new OperationNotSupportedException("The HBase Table implementation does not support " +
                        "conditions which do not contain at lease one reference to a table column. Please check your " +
                        "query and try again.");
            }
            //TODO 
        });
    }

    public void beginVisitCompare(Compare.Operator operator) {
    }

    public void endVisitCompare(Compare.Operator operator) {
    }

    public void beginVisitCompareLeftOperand(Compare.Operator operator) {
    }

    public void endVisitCompareLeftOperand(Compare.Operator operator) {
    }

    public void beginVisitCompareRightOperand(Compare.Operator operator) {
    }

    public void endVisitCompareRightOperand(Compare.Operator operator) {
    }

    public void beginVisitConstant(Object value, Attribute.Type type) {
    }

    public void endVisitConstant(Object value, Attribute.Type type) {
    }

    public void beginVisitStreamVariable(String id, String streamId, String attributeName, Attribute.Type type) {
    }

    public void endVisitStreamVariable(String id, String streamId, String attributeName, Attribute.Type type) {
    }

    public void beginVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {
    }

    public void endVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {
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
