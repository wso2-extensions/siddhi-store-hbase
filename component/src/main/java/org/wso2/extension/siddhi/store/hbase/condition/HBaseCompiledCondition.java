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

import org.apache.hadoop.hbase.filter.Filter;
import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation class of {@link CompiledCondition} corresponding to the HBase Event Table.
 * Maintains the conditions returned by the ExpressionVisitor as well as as a set of boolean values for inferring
 * states to be used at runtime.
 */
public class HBaseCompiledCondition implements CompiledCondition {

    private List<BasicCompareOperation> operations;
    private List<Filter> filters;

    private boolean readOnlyCondition;
    private boolean allKeyEquals;

    public HBaseCompiledCondition(List<BasicCompareOperation> conditions, List<Filter> filters,
                                  boolean readOnlyCondition, boolean allKeyEquals) {
        this.operations = conditions;
        this.filters = filters;
        this.readOnlyCondition = readOnlyCondition;
        this.allKeyEquals = allKeyEquals;
    }

    public HBaseCompiledCondition() {
        this.operations = new ArrayList<>();
        this.filters = new ArrayList<>();
        this.readOnlyCondition = false;
        this.allKeyEquals = false;
    }

    @Override
    public CompiledCondition cloneCompiledCondition(String s) {
        return new HBaseCompiledCondition(this.operations, this.filters, this.readOnlyCondition, this.allKeyEquals);
    }

    public List<BasicCompareOperation> getOperations() {
        return operations;
    }

    public List<Filter> getFilters() {
        return filters;
    }

    public boolean isReadOnlyCondition() {
        return readOnlyCondition;
    }

    public boolean isAllKeyEquals() {
        return allKeyEquals;
    }

}
