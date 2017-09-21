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

    public List<BasicCompareOperation> gtConditions() {
        this.preProcessConditions();
        return compareOps;
    }

    private void preProcessConditions() {

        this.allKeyEquals = true;
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
            //TODO todo todotodotodo todoodo doooo
        });
    }

    public void beginVisitAnd() {
    }

    public void endVisitAnd() {

    }

    public void beginVisitAndLeftOperand() {
    }

    public void endVisitAndLeftOperand() {
    }

    public void beginVisitAndRightOperand() {
    }

    public void endVisitAndRightOperand() {
    }

    public void beginVisitOr() {
    }

    public void endVisitOr() {
    }

    public void beginVisitOrLeftOperand() {
    }

    public void endVisitOrLeftOperand() {
    }

    public void beginVisitOrRightOperand() {
    }

    public void endVisitOrRightOperand() {
    }

    public void beginVisitNot() {
    }

    public void endVisitNot() {
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

    public void beginVisitIsNull(String streamId) {
    }

    public void endVisitIsNull(String streamId) {
    }

    public void beginVisitIn(String storeId) {
    }

    public void endVisitIn(String storeId) {
    }

    public void beginVisitConstant(Object value, Attribute.Type type) {
    }

    public void endVisitConstant(Object value, Attribute.Type type) {
    }

    public void beginVisitMath(MathOperator mathOperator) {
    }

    public void endVisitMath(MathOperator mathOperator) {
    }

    public void beginVisitMathLeftOperand(MathOperator mathOperator) {
    }

    public void endVisitMathLeftOperand(MathOperator mathOperator) {
    }

    public void beginVisitMathRightOperand(MathOperator mathOperator) {
    }

    public void endVisitMathRightOperand(MathOperator mathOperator) {
    }

    public void beginVisitAttributeFunction(String namespace, String functionName) {
    }

    public void endVisitAttributeFunction(String namespace, String functionName) {
    }

    public void beginVisitParameterAttributeFunction(int index) {
    }

    public void endVisitParameterAttributeFunction(int index) {
    }

    public void beginVisitStreamVariable(String id, String streamId, String attributeName, Attribute.Type type) {
    }

    public void endVisitStreamVariable(String id, String streamId, String attributeName, Attribute.Type type) {
    }

    public void beginVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {
    }

    public void endVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {
    }

}
