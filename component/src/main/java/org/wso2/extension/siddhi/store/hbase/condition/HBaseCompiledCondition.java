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

import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;

import java.util.List;
import java.util.stream.Collectors;

public class HBaseCompiledCondition implements CompiledCondition {

    private List<BasicCompareOperation> operations;

    private boolean readOnlyCondition;
    private boolean allKeyEquals;

    public HBaseCompiledCondition(List<BasicCompareOperation> conditions, boolean readOnlyCondition,
                                  boolean allKeyEquals) {
        this.operations = conditions;
        this.readOnlyCondition = readOnlyCondition;
        this.allKeyEquals = allKeyEquals;
    }

    @Override
    public CompiledCondition cloneCompiledCondition(String s) {
        return new HBaseCompiledCondition(this.operations, this.readOnlyCondition, this.allKeyEquals);
    }

    public List<BasicCompareOperation> getOperations() {
        return operations;
    }

    public boolean isReadOnlyCondition() {
        return readOnlyCondition;
    }

    public boolean isAllKeyEquals() {
        return allKeyEquals;
    }

    public String toString() {
        return operations.stream()
                .map(Object::toString)
                .collect(Collectors.joining(", "));
    }
}
