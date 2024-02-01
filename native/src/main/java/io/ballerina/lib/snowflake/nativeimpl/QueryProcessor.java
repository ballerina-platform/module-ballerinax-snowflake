/*
 * Copyright (c) 2022, WSO2 LLC. (http://www.wso2.com) All Rights Reserved.
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
 */

package io.ballerina.lib.snowflake.nativeimpl;

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BStream;
import io.ballerina.runtime.api.values.BTypedesc;
import io.ballerina.stdlib.sql.parameterprocessor.DefaultResultParameterProcessor;
import io.ballerina.stdlib.sql.parameterprocessor.DefaultStatementParameterProcessor;

/**
 * This class provides the query processing implementation which executes sql queries.
 * 
 * @since 1.0.0
 */
public class QueryProcessor {

    private QueryProcessor() {
    }

    public static BStream nativeQuery(Environment env, BObject client, BObject paramSQLString,
                                      BTypedesc recordType) {
        DefaultStatementParameterProcessor statementParametersProcessor = DefaultStatementParameterProcessor
                .getInstance();
        DefaultResultParameterProcessor resultParametersProcessor = DefaultResultParameterProcessor
                .getInstance();
        return io.ballerina.stdlib.sql.nativeimpl.QueryProcessor.nativeQuery(env, client, paramSQLString, recordType,
                statementParametersProcessor, resultParametersProcessor);
    }

    public static Object nativeQueryRow(Environment env, BObject client, BObject paramSQLString, BTypedesc recordType) {
        DefaultStatementParameterProcessor statementParametersProcessor = DefaultStatementParameterProcessor
                .getInstance();
        DefaultResultParameterProcessor resultParametersProcessor = DefaultResultParameterProcessor
                .getInstance();
        return io.ballerina.stdlib.sql.nativeimpl.QueryProcessor.nativeQueryRow(env, client, paramSQLString, recordType,
                statementParametersProcessor, resultParametersProcessor);
    }
}
