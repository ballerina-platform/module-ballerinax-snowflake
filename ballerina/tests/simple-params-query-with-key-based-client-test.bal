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
function setupDataTableWithKeyBasedClient() returns error? {
    _ = check snowflakeKeyBasedClient->execute(`CREATE DATABASE IF NOT EXISTS TEST_DB_1`);
    _ = check snowflakeKeyBasedClient->execute(`CREATE TEMPORARY TABLE IF NOT EXISTS TEST_DB_1.PUBLIC.DATA_TABLE(row_id INTEGER, int_type INTEGER, long_type BIGINT,
                                          float_type   FLOAT,
                                          double_type  DOUBLE,
                                          boolean_type BOOLEAN,
                                          string_type  VARCHAR(50),
                                          decimal_type DECIMAL(20, 2),
                                          PRIMARY KEY (row_id)
                                        );`);
    _ = check snowflakeKeyBasedClient->execute(`INSERT INTO TEST_DB_1.PUBLIC.DATA_TABLE (row_id, int_type, long_type, float_type, double_type, boolean_type, string_type, decimal_type)
                                          VALUES(1, 1, 9223372036854774807, 123.34, 2139095039, TRUE, 'Hello', 23.45)`);
    _ = check snowflakeKeyBasedClient->execute(`INSERT INTO TEST_DB_1.PUBLIC.DATA_TABLE (row_id, int_type, long_type, float_type, double_type, boolean_type, string_type, decimal_type)
                                          VALUES(3, 1, 9372036854774807, 124.34, 29095039, false, '1', 25.45)`);
    _ = check snowflakeKeyBasedClient->execute(`INSERT INTO TEST_DB_1.PUBLIC.DATA_TABLE (row_id) VALUES (2)`);
    return ();
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function querySingleIntParamWithKeyBasedClient() returns error? {
    int rowId = 1;
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB_1.PUBLIC.DATA_TABLE WHERE row_id = ${rowId}`;
    stream<DataTable, error?> resultStream = snowflakeKeyBasedClient->query(sqlQuery);
    DataTable[] data = check from var result in resultStream
    select result;

    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateDataTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryDoubleIntParamWithKeyBasedClient() returns error? {
    int rowId = 1;
    int intType = 1;
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB_1.PUBLIC.DATA_TABLE WHERE row_id = ${rowId} AND int_type =  ${intType}`;
    stream<DataTable, error?> resultStream = snowflakeKeyBasedClient->query(sqlQuery);
        DataTable[] data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateDataTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryIntAndLongParamWithKeyBasedClient() returns error? {
    int rowId = 1;
    int longType = 9223372036854774807;
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB_1.PUBLIC.DATA_TABLE WHERE row_id = ${rowId} AND long_type = ${longType}`;
    stream<DataTable, error?> resultStream = snowflakeKeyBasedClient->query(sqlQuery);
        DataTable[] data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateDataTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryStringParamWithKeyBasedClient() returns error? {
    string stringType = "Hello";
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB_1.PUBLIC.DATA_TABLE WHERE string_type = ${stringType}`;
    stream<DataTable, error?> resultStream = snowflakeKeyBasedClient->query(sqlQuery);
        DataTable[] data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateDataTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryIntAndStringParamWithKeyBasedClient() returns error? {
    string stringType = "Hello";
    int rowId = 1;
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB_1.PUBLIC.DATA_TABLE WHERE string_type = ${stringType} AND row_id = ${rowId}`;
    stream<DataTable, error?> resultStream = snowflakeKeyBasedClient->query(sqlQuery);
        DataTable[] data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateDataTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryDoubleParamWithKeyBasedClient() returns error? {
    float doubleType = 2139095039.0;
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB_1.PUBLIC.DATA_TABLE WHERE double_type = ${doubleType}`;
    stream<DataTable, error?> resultStream = snowflakeKeyBasedClient->query(sqlQuery);
        DataTable[] data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateDataTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryFloatParamWithKeyBasedClient() returns error? {
    float floatType = 123.34;
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB_1.PUBLIC.DATA_TABLE WHERE float_type = ${floatType}`;
    stream<DataTable, error?> resultStream = snowflakeKeyBasedClient->query(sqlQuery);
        DataTable[] data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateDataTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryDoubleAndFloatParamWithKeyBasedClient() returns error? {
    float floatType = 123.34;
    float doubleType = 2139095039.0;
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB_1.PUBLIC.DATA_TABLE WHERE float_type = ${floatType}
                                                                    and double_type = ${doubleType}`;
    stream<DataTable, error?> resultStream = snowflakeKeyBasedClient->query(sqlQuery);
        DataTable[] data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateDataTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryDecimalParamWithKeyBasedClient() returns error? {
    decimal decimalValue = 23.45;
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB_1.PUBLIC.DATA_TABLE WHERE decimal_type = ${decimalValue}`;
    stream<DataTable, error?> resultStream = snowflakeKeyBasedClient->query(sqlQuery);
        DataTable[] data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateDataTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryDecimalAnFloatParamWithKeyBasedClient() returns error? {
    decimal decimalValue = 23.45;
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB_1.PUBLIC.DATA_TABLE WHERE decimal_type = ${decimalValue}
                                                                    and double_type = 2139095039.0`;
    stream<DataTable, error?> resultStream = snowflakeKeyBasedClient->query(sqlQuery);
        DataTable[] data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateDataTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeVarcharStringParamWithKeyBasedClient() returns error? {
    sql:VarcharValue typeVal = new ("Hello");
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB_1.PUBLIC.DATA_TABLE WHERE string_type = ${typeVal}`;
    stream<DataTable, error?> resultStream = snowflakeKeyBasedClient->query(sqlQuery);
        DataTable[] data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateDataTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeCharStringParamWithKeyBasedClient() returns error? {
    sql:CharValue typeVal = new ("Hello");
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB_1.PUBLIC.DATA_TABLE WHERE string_type = ${typeVal}`;
    stream<DataTable, error?> resultStream = snowflakeKeyBasedClient->query(sqlQuery);
        DataTable[] data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateDataTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeNCharStringParamWithKeyBasedClient() returns error? {
    sql:CharValue typeVal = new ("Hello");
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB_1.PUBLIC.DATA_TABLE WHERE string_type = ${typeVal}`;
    stream<DataTable, error?> resultStream = snowflakeKeyBasedClient->query(sqlQuery);
        DataTable[] data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateDataTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeNVarCharStringParamWithKeyBasedClient() returns error? {
    sql:VarcharValue typeVal = new ("Hello");
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB_1.PUBLIC.DATA_TABLE WHERE string_type = ${typeVal}`;
    stream<DataTable, error?> resultStream = snowflakeKeyBasedClient->query(sqlQuery);
        DataTable[] data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateDataTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeVarCharIntegerParamWithKeyBasedClient() returns error? {
    sql:VarcharValue typeVal = new ("1");
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB_1.PUBLIC.DATA_TABLE WHERE string_type = ${typeVal}`;

    decimal decimalVal = 25.45;
    stream<DataTable, error?> resultStream = snowflakeKeyBasedClient->query(sqlQuery);
        DataTable[] data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    test:assertEquals(data[0].int_type, 1);
    test:assertEquals(data[0].long_type, 9372036854774807);
    test:assertEquals(data[0].double_type, <float>29095039);
    test:assertEquals(data[0].boolean_type, false);
    test:assertEquals(data[0].decimal_type, decimalVal);
    test:assertEquals(data[0].string_type, "1");
    test:assertEquals(data[0].row_id, 3);
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypBooleanBooleanParamWithKeyBasedClient() returns error? {
    sql:BooleanValue typeVal = new (true);
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB_1.PUBLIC.DATA_TABLE WHERE boolean_type = ${typeVal}`;
    stream<DataTable, error?> resultStream = snowflakeKeyBasedClient->query(sqlQuery);
        DataTable[] data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateDataTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypBitIntParamWithKeyBasedClient() returns error? {
    sql:BitValue typeVal = new (1);
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB_1.PUBLIC.DATA_TABLE WHERE boolean_type = ${typeVal}`;
    stream<DataTable, error?> resultStream = snowflakeKeyBasedClient->query(sqlQuery);
        DataTable[] data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateDataTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypBitStringParamWithKeyBasedClient() returns error? {
    sql:BitValue typeVal = new (true);
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB_1.PUBLIC.DATA_TABLE WHERE boolean_type = ${typeVal}`;
    stream<DataTable, error?> resultStream = snowflakeKeyBasedClient->query(sqlQuery);
        DataTable[] data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateDataTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypBitInvalidIntParamWithKeyBasedClient() returns error? {
    sql:BitValue typeVal = new (12);
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB_1.PUBLIC.DATA_TABLE WHERE boolean_type = ${typeVal}`;
    stream<DataTable, error?> resultStream = snowflakeKeyBasedClient->query(sqlQuery);
        DataTable[]|error returnVal = from var result in resultStream
        select result;
    test:assertTrue(returnVal is error);
    error dbError = <error>returnVal;
    test:assertEquals(dbError.message(), "Only 1 or 0 can be passed for BitValue SQL Type, but found :12");
}

isolated function validateDataTableResultWithKeyBasedClient(DataTable returnData) {
    decimal decimalVal = 23.45;
    test:assertEquals(returnData.row_id, 1);
    test:assertEquals(returnData.int_type, 1);
    test:assertEquals(returnData.long_type, 9223372036854774807);
    test:assertEquals(returnData.double_type, <float>2139095039);
    test:assertEquals(returnData.boolean_type, true);
    test:assertEquals(returnData.decimal_type, decimalVal);
    test:assertEquals(returnData.string_type, "Hello");
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryRecordWithKeyBasedClient() returns sql:Error? {
    int rowId = 1;
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB_1.PUBLIC.DATA_TABLE WHERE row_id = ${rowId}`;
    DataTable queryResult = check snowflakeKeyBasedClient->queryRow(sqlQuery);
    validateDataTableResult(queryResult);
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryRecordNegativeWithKeyBasedClient() returns sql:Error? {
    int rowId = 999;
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB_1.PUBLIC.DATA_TABLE WHERE row_id = ${rowId}`;
    DataTable|sql:Error queryResult = snowflakeKeyBasedClient->queryRow(sqlQuery);
    if queryResult is sql:Error {
        test:assertTrue(queryResult is sql:NoRowsError);
        test:assertTrue(queryResult.message().endsWith("Query did not retrieve any rows."), "Incorrect error message");
    } else {
        test:assertFail("Expected no rows error with empty query result.");
    }
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryRecordNegative3WithKeyBasedClient() returns error? {
    int rowId = 1;
    sql:ParameterizedQuery sqlQuery = `SELECT row_id, invalid_column_name from TEST_DB_1.PUBLIC.DATA_TABLE WHERE row_id = ${rowId}`;
    DataTable|error queryResult = snowflakeKeyBasedClient->queryRow(sqlQuery);
    if queryResult is error {
        test:assertTrue(queryResult.message().endsWith("invalid identifier 'INVALID_COLUMN_NAME'."),
                        "Incorrect error message");
    } else {
        test:assertFail("Expected error when querying with invalid column name.");
    }
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryValueWithKeyBasedClient() returns error? {
    int count = check snowflakeKeyBasedClient->queryRow(`SELECT COUNT(*) FROM TEST_DB_1.PUBLIC.DATA_TABLE`);
    test:assertEquals(count, 3);
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryValueNegative1WithKeyBasedClient() returns error? {
    int rowId = 1;
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB_1.PUBLIC.DATA_TABLE WHERE row_id = ${rowId}`;
    int|error queryResult = snowflakeKeyBasedClient->queryRow(sqlQuery);
    if queryResult is error {
        test:assertTrue(queryResult is sql:TypeMismatchError, "Incorrect error type");
        test:assertEquals(queryResult.message(), "Expected type to be 'int' but found 'record{}'.");
    } else {
        test:assertFail("Expected error when query result contains multiple columns.");
    }
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryValueNegative2WithKeyBasedClient() returns error? {
    int rowId = 1;
    sql:ParameterizedQuery sqlQuery = `SELECT string_type from TEST_DB_1.PUBLIC.DATA_TABLE WHERE row_id = ${rowId}`;
    int|error queryResult = snowflakeKeyBasedClient->queryRow(sqlQuery);
    if queryResult is error {
        test:assertEquals(queryResult.message(),
                        "SQL Type 'VARCHAR' cannot be converted to ballerina type 'int'.",
                        "Incorrect error message");
    } else {
        test:assertFail("Expected error when query returns unexpected result type.");
    }
}
