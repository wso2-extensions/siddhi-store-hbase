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

import org.wso2.siddhi.query.api.definition.Attribute;

public abstract class Operand {
    protected Attribute.Type type;

    public Attribute.Type getType() {
        return type;
    }

    public static class StreamVariable extends Operand {
        private String name;

        StreamVariable(String name, Attribute.Type type) {
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return name;
        }
    }

    public static class Constant extends Operand {
        private Object value;

        Constant(Object value, Attribute.Type type) {
            this.value = value;
            this.type = type;
        }

        public Object getValue() {
            return value;
        }
    }

    public static class StoreVariable extends Operand {
        private String name;

        StoreVariable(String name, Attribute.Type type) {
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return name;
        }
    }
}