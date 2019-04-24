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

import io.siddhi.query.api.expression.condition.Compare;

/**
 * Class that denotes a simple compare operation that can be undertaken by the HBase instance. This contains a couple
 * of operands and one operator.
 */
public class BasicCompareOperation {

    private Compare.Operator operator;
    private Operand operand1;
    private Operand operand2;

    public BasicCompareOperation() {
        this.operand1 = null;
        this.operand2 = null;
        this.operator = null;
    }

    public Compare.Operator getOperator() {
        return operator;
    }

    public void setOperator(Compare.Operator operator) {
        this.operator = operator;
    }

    public Operand getOperand1() {
        return operand1;
    }

    public void setOperand1(Operand operand1) {
        this.operand1 = operand1;
    }

    public Operand getOperand2() {
        return operand2;
    }

    public void setOperand2(Operand operand2) {
        this.operand2 = operand2;
    }

    public boolean isInvalid() {
        return ((this.operand1 == null) || (this.operand2 == null) || (this.operator == null));
    }

}
