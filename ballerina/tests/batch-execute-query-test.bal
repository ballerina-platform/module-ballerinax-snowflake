// Copyright (c) 2023, WSO2 LLC. (http://www.wso2.com) All Rights Reserved.
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

import ballerina/sql;
import ballerina/test;
import ballerinax/snowflake.driver as _;

@test:BeforeSuite
function setupBatchTable() returns error? {
    _ = check snowflakeClient->execute(`CREATE DATABASE IF NOT EXISTS TEST_DB`);
    _ = check snowflakeClient->execute(`CREATE TEMPORARY TABLE IF NOT EXISTS TEST_DB.PUBLIC.BATCH_TABLE (
                                                                   id INT AUTOINCREMENT,
                                                                   int_type     INTEGER NOT NULL,
                                                                   long_type    BIGINT,
                                                                   float_type   FLOAT,
                                                                   UNIQUE (int_type),
                                                                   PRIMARY KEY (id)
                                                               )`);
    _ = check snowflakeClient->execute(`INSERT INTO TEST_DB.PUBLIC.BATCH_TABLE (int_type, long_type, float_type)
                                                                                 VALUES(1, 9223372036854774807, 123.34)`);
    _ = check snowflakeClient->execute(`INSERT INTO TEST_DB.PUBLIC.BATCH_TABLE (int_type, long_type, float_type)
                                                                                 VALUES(2, 9372036854774807, 124.34)`);
    return ();
}

@test:Config {
    groups: ["batch-execute"]
}
function batchInsertIntoDataTable() returns error? {
    var data = [
        {intVal: 3, longVal: 9223372036854774807, floatVal: 123.34},
        {intVal: 4, longVal: 9223372036854774807, floatVal: 123.34},
        {intVal: 5, longVal: 9223372036854774807, floatVal: 123.34}
    ];
    sql:ParameterizedQuery[] sqlQueries =
        from var row in data
        select `INSERT INTO TEST_DB.PUBLIC.BATCH_TABLE (int_type, long_type, float_type) VALUES (${row.intVal}, ${row.longVal}, ${row.floatVal})`;
    sql:ExecutionResult[] result = check snowflakeClient->batchExecute(sqlQueries);
    validateBatchExecutionResult(result, [1, 1, 1], [2,3,4]);
}

@test:Config {
    groups: ["batch-execute"],
    dependsOn: [batchInsertIntoDataTable]
}
function batchInsertIntoDataTable2() returns error? {
    int intType = 6;
    sql:ParameterizedQuery sqlQuery = `INSERT INTO TEST_DB.PUBLIC.BATCH_TABLE (int_type) VALUES(${intType})`;
    sql:ParameterizedQuery[] sqlQueries = [sqlQuery];

    sql:ExecutionResult[] result = check snowflakeClient->batchExecute(sqlQueries);
    validateBatchExecutionResult(result, [1], [5]);
}

@test:Config {
    groups: ["batch-execute"],
    dependsOn: [batchInsertIntoDataTable2]
}
function batchInsertIntoDataTableFailure() {
    var data = [
        {intVal: 7, longVal: 9223372036854774807, floatVal: 123.34},
        {intVal: (), longVal: 9223372036854774807, floatVal: 123.34},
        {intVal: 9, longVal: 9223372036854774807, floatVal: 123.34}
    ];
    sql:ParameterizedQuery[] sqlQueries =
        from var row in data
        select `INSERT INTO TEST_DB.PUBLIC.BATCH_TABLE (int_type, long_type, float_type) VALUES (${row.intVal}, ${row.longVal}, ${row.floatVal})`;
    sql:ExecutionResult[]|error result = snowflakeClient->batchExecute(sqlQueries);
    test:assertTrue(result is sql:DatabaseError);

    if result is sql:DatabaseError {
        test:assertTrue(result.message().endsWith("NULL result in a non-nullable column."),
                                                      "Invalid error message received.");
    } else {
        test:assertFail("Database Error expected.");
    }
}

isolated function validateBatchExecutionResult(sql:ExecutionResult[] results, int[] rowCount, int[] lastId) {
    test:assertEquals(results.length(), rowCount.length());

    int i = 0;
    while i < results.length() {
        test:assertEquals(results[i].affectedRowCount, rowCount[i]);
        int|string? lastInsertIdVal = results[i].lastInsertId;
        if lastId[i] == -1 {
            test:assertNotEquals(lastInsertIdVal, ());
        } else if lastInsertIdVal is int {
            test:assertTrue(lastInsertIdVal > 1, "Last Insert Id is nil.");
        }
        i = i + 1;
    }
}
