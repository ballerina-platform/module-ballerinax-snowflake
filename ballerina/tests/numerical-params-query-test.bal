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
function setupNumericTypesTable() returns error? {
    _ = check snowflakeClient->execute(`CREATE DATABASE IF NOT EXISTS TEST_DB`);
    _ = check snowflakeClient->execute(`CREATE TEMPORARY TABLE IF NOT EXISTS TEST_DB.PUBLIC.NUMERIC_TYPES (
                                                                                id INT AUTOINCREMENT,
                                                                                int_type INT NOT NULL,
                                                                                bigint_type BIGINT NOT NULL,
                                                                                smallint_type SMALLINT NOT NULL ,
                                                                                mediumint_type INTEGER NOT NULL ,
                                                                                tinyint_type TINYINT NOT NULL ,
                                                                                bit_type BOOLEAN NOT NULL ,
                                                                                decimal_type DECIMAL(10,3) NOT NULL ,
                                                                                numeric_type NUMERIC(10,3) NOT NULL ,
                                                                                float_type FLOAT NOT NULL ,
                                                                                real_type REAL NOT NULL ,
                                                                                PRIMARY KEY (id)
                                                                             )`);
    _ = check snowflakeClient->execute(`INSERT INTO TEST_DB.PUBLIC.NUMERIC_TYPES (id, int_type, bigint_type, smallint_type, mediumint_type, tinyint_type, bit_type, decimal_type, numeric_type,
                                            float_type, real_type) VALUES (1, 2147483647, 9223372036854774807, 32767, 8388607, 127, 1, 1234.567, 1234.567, 1234.567,
                                            1234.567)`);
    _ = check snowflakeClient->execute(`INSERT INTO TEST_DB.PUBLIC.NUMERIC_TYPES (id, int_type, bigint_type, smallint_type, mediumint_type, tinyint_type, bit_type, decimal_type, numeric_type,
                                            float_type, real_type) VALUES (2, 2147483647, 9223372036854774807, 32767, 8388607, 127, 1, 1234, 1234, 1234,
                                            1234)`);
}

@test:Config {
    groups: ["query", "query-numerical-params"]
}
function queryTypeIntIntParam() returns error? {
    sql:IntegerValue typeVal = new (2147483647);
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB.PUBLIC.NUMERIC_TYPES WHERE int_type = ${typeVal}`;
    stream<NumericTypes, error?> resultStream = snowflakeClient->query(sqlQuery);
    NumericTypes[] data = check from var result in resultStream
    select result;

    test:assertEquals(data.length(), 2, "Invalid number of records returned");
    validateNumericTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-numerical-params"]
}
function queryTypeTinyIntIntParam() returns error? {
    sql:SmallIntValue typeVal = new (127);
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB.PUBLIC.NUMERIC_TYPES WHERE tinyint_type = ${typeVal}`;
    stream<NumericTypes, error?> resultStream = snowflakeClient->query(sqlQuery);
    NumericTypes[] data = check from var result in resultStream
    select result;

    test:assertEquals(data.length(), 2, "Invalid number of records returned");
    validateNumericTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-numerical-params"]
}
function queryTypeSmallIntIntParam() returns error? {
    sql:SmallIntValue typeVal = new (32767);
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB.PUBLIC.NUMERIC_TYPES WHERE smallint_type = ${typeVal}`;
    stream<NumericTypes, error?> resultStream = snowflakeClient->query(sqlQuery);
    NumericTypes[] data = check from var result in resultStream
    select result;

    test:assertEquals(data.length(), 2, "Invalid number of records returned");
    validateNumericTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-numerical-params"]
}
function queryTypeMediumIntIntParam() returns error? {
    sql:IntegerValue typeVal = new (8388607);
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB.PUBLIC.NUMERIC_TYPES WHERE mediumint_type = ${typeVal}`;
    stream<NumericTypes, error?> resultStream = snowflakeClient->query(sqlQuery);
    NumericTypes[] data = check from var result in resultStream
    select result;

    test:assertEquals(data.length(), 2, "Invalid number of records returned");
    validateNumericTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-numerical-params"]
}
function queryTypeBigIntIntParam() returns error? {
    sql:BigIntValue typeVal = new (9223372036854774807);
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB.PUBLIC.NUMERIC_TYPES WHERE bigint_type = ${typeVal}`;
    stream<NumericTypes, error?> resultStream = snowflakeClient->query(sqlQuery);
    NumericTypes[] data = check from var result in resultStream
    select result;

    test:assertEquals(data.length(), 2, "Invalid number of records returned");
    validateNumericTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-numerical-params"]
}
function queryTypeDoubleDoubleParam() returns error? {
    sql:DoubleValue typeVal = new (1234.567);
    sql:DoubleValue typeVal2 = new (1234.57);
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB.PUBLIC.NUMERIC_TYPES WHERE float_type between ${typeVal} AND ${typeVal2}`;
    stream<NumericTypes, error?> resultStream = snowflakeClient->query(sqlQuery);
    NumericTypes[] data = check from var result in resultStream
    select result;

    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateNumericTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-numerical-params"]
}
function queryTypeDoubleIntParam() returns error? {
    sql:DoubleValue typeVal = new (1234);
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB.PUBLIC.NUMERIC_TYPES WHERE float_type = ${typeVal}`;
    stream<NumericTypes, error?> resultStream = snowflakeClient->query(sqlQuery);
    NumericTypes[] data = check from var result in resultStream
    select result;

    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    test:assertEquals(data[0].length(), 11);
    test:assertEquals(data[0].id, 2);
    test:assertEquals(data[0].real_type, 1234.0);

}

@test:Config {
    groups: ["query", "query-numerical-params"]
}
function queryTypeDoubleDecimalParam() returns error? {
    decimal decimalVal = 1234.567;
    decimal decimalVal2 = 1234.57;
    sql:DoubleValue typeVal = new (decimalVal);
    sql:DoubleValue typeVal2 = new (decimalVal2);
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB.PUBLIC.NUMERIC_TYPES WHERE float_type between ${typeVal} AND ${typeVal2}`;
    stream<NumericTypes, error?> resultStream = snowflakeClient->query(sqlQuery);
    NumericTypes[] data = check from var result in resultStream
    select result;

    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateNumericTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-numerical-params"]
}
function queryTypeFloatDoubleParam() returns error? {
    sql:DoubleValue typeVal1 = new (1234.567);
    sql:DoubleValue typeVal2 = new (1234.57);
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB.PUBLIC.NUMERIC_TYPES WHERE float_type between ${typeVal1} AND ${typeVal2}`;
    stream<NumericTypes, error?> resultStream = snowflakeClient->query(sqlQuery);
    NumericTypes[] data = check from var result in resultStream
    select result;

    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateNumericTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-numerical-params"]
}
function queryTypeRealDoubleParam() returns error? {
    sql:RealValue typeVal = new (1234.567);
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB.PUBLIC.NUMERIC_TYPES WHERE real_type = ${typeVal}`;
    stream<NumericTypes, error?> resultStream = snowflakeClient->query(sqlQuery);
    NumericTypes[] data = check from var result in resultStream
    select result;

    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateNumericTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-numerical-params"]
}
function queryTypeNumericDoubleParam() returns error? {
    sql:NumericValue typeVal = new (1234.567);
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB.PUBLIC.NUMERIC_TYPES WHERE numeric_type = ${typeVal}`;
    stream<NumericTypes, error?> resultStream = snowflakeClient->query(sqlQuery);
    NumericTypes[] data = check from var result in resultStream
    select result;

    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateNumericTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-numerical-params"]
}
function queryTypeNumericIntParam() returns error? {
    sql:NumericValue typeVal = new (1234);
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB.PUBLIC.NUMERIC_TYPES WHERE numeric_type = ${typeVal}`;
    stream<NumericTypes, error?> resultStream = snowflakeClient->query(sqlQuery);
    NumericTypes[] data = check from var result in resultStream
    select result;

    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    test:assertEquals(data[0].length(), 11);
    test:assertEquals(data[0].id, 2);
    test:assertEquals(data[0].real_type, 1234.0);
}

@test:Config {
    groups: ["query", "query-numerical-params"]
}
function queryTypeNumericDecimalParam() returns error? {
    decimal decimalVal = 1234.567;
    sql:NumericValue typeVal = new (decimalVal);
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB.PUBLIC.NUMERIC_TYPES WHERE numeric_type = ${typeVal}`;
    stream<NumericTypes, error?> resultStream = snowflakeClient->query(sqlQuery);
    NumericTypes[] data = check from var result in resultStream
    select result;

    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateNumericTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-numerical-params"]
}
function queryTypeDecimalDoubleParam() returns error? {
    sql:DecimalValue typeVal = new (1234.567);
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB.PUBLIC.NUMERIC_TYPES WHERE decimal_type = ${typeVal}`;
    stream<NumericTypes, error?> resultStream = snowflakeClient->query(sqlQuery);
    NumericTypes[] data = check from var result in resultStream
    select result;

    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateNumericTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-numerical-params"]
}
function queryTypeDecimalDecimalParam() returns error? {
    decimal decimalVal = 1234.567;
    sql:DecimalValue typeVal = new (decimalVal);
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB.PUBLIC.NUMERIC_TYPES WHERE decimal_type = ${typeVal}`;
    stream<NumericTypes, error?> resultStream = snowflakeClient->query(sqlQuery);
    NumericTypes[] data = check from var result in resultStream
    select result;

    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateNumericTableResult(data[0]);
}

type NumericTypes record {
    int id;
    int int_type;
    int bigint_type;
    int smallint_type;
    int mediumint_type;
    int tinyint_type;
    boolean bit_type;
    float real_type;
    decimal decimal_type;
    decimal numeric_type;
    float float_type;
};

isolated function validateNumericTableResult(NumericTypes returnData) {
    test:assertEquals(returnData.id, 1);
    test:assertEquals(returnData.int_type, 2147483647);
    test:assertEquals(returnData.bigint_type, 9223372036854774807);
    test:assertEquals(returnData.smallint_type, 32767);
    test:assertEquals(returnData.mediumint_type, 8388607);
    test:assertEquals(returnData.tinyint_type, 127);
    test:assertEquals(returnData.bit_type, true);
    test:assertEquals(returnData.real_type, 1234.567);
}

type NumericInvalidColumn record {|
    int num_id;
    int int_type;
    int bigint_type;
    int smallint_type;
    int tinyint_type;
    boolean bit_type;
    decimal decimal_type;
    decimal numeric_type;
    float float_type;
    float real_type;
|};

@test:Config {
    groups: ["query", "query-numeric-params"]
}
function testQueryNumericInvalidColumnRecord() returns error? {
    stream<NumericInvalidColumn, sql:Error?> streamData = snowflakeClient->query(`SELECT * FROM TEST_DB.PUBLIC.NUMERIC_TYPES`);
    record {|NumericInvalidColumn value;|}|sql:Error? data = streamData.next();
    check streamData.close();
    test:assertTrue(data is error);
    error dbError = <error>data;
    test:assertEquals(dbError.message(), "No mapping field found for SQL table column 'ID' in the record type 'NumericInvalidColumn'", "Error message differs");
}

type NumericOptionalType record {
    int? id;
    int? int_type;
    int? bigint_type;
    int? smallint_type;
    int? tinyint_type;
    boolean? bit_type;
    decimal? decimal_type;
    decimal? numeric_type;
    float? float_type;
    float? real_type;
};

@test:Config {
    groups: ["query", "query-numeric-params"]
}
function testQueryNumericOptionalTypeRecord() returns error? {
    stream<NumericOptionalType, sql:Error?> streamData = snowflakeClient->query(`SELECT * FROM TEST_DB.PUBLIC.NUMERIC_TYPES`);
    record {|NumericOptionalType value;|}? data = check streamData.next();
    check streamData.close();
    NumericOptionalType? returnData = data?.value;

    test:assertEquals(returnData?.id, 1);
    test:assertEquals(returnData?.int_type, 2147483647);
    test:assertEquals(returnData?.bigint_type, 9223372036854774807);
    test:assertEquals(returnData?.smallint_type, 32767);
    test:assertEquals(returnData?.tinyint_type, 127);
    test:assertEquals(returnData?.bit_type, true);
    test:assertEquals(returnData?.real_type, 1234.567);
    test:assertTrue(returnData?.decimal_type is decimal);
    test:assertTrue(returnData?.numeric_type is decimal);
    test:assertTrue(returnData?.float_type is float);
}

type NumericUnionType record {
    int|string id;
    int|string int_type;
    int|string bigint_type;
    int|string smallint_type;
    int|string tinyint_type;
    boolean|string bit_type;
    int|decimal decimal_type;
    decimal|int numeric_type;
    decimal|float? float_type;
    decimal|float? real_type;
};

@test:Config {
    groups: ["query", "query-numeric-params"]
}
function testQueryNumericUnionTypeRecord() returns error? {
    stream<NumericUnionType, sql:Error?> streamData = snowflakeClient->query(`SELECT * FROM TEST_DB.PUBLIC.NUMERIC_TYPES`);
    record {|NumericUnionType value;|}? data = check streamData.next();
    check streamData.close();
    NumericUnionType? returnData = data?.value;

    test:assertEquals(returnData?.id, 1);
    test:assertEquals(returnData?.int_type, 2147483647);
    test:assertEquals(returnData?.bigint_type, 9223372036854774807);
    test:assertEquals(returnData?.smallint_type, 32767);
    test:assertEquals(returnData?.tinyint_type, 127);
    test:assertEquals(returnData?.bit_type, true);
    test:assertEquals(returnData?.real_type, 1234.567);
    test:assertTrue(returnData?.decimal_type is decimal);
    test:assertTrue(returnData?.numeric_type is decimal);
    test:assertTrue(returnData?.float_type is float);

}

type NumericStringType record {
    string? id;
    string? int_type;
    string? bigint_type;
    string? smallint_type;
    string? tinyint_type;
    string? bit_type;
    string? decimal_type;
    string? numeric_type;
    string? float_type;
    string? real_type;
};

@test:Config {
    groups: ["query", "query-numeric-params"]
}
function testQueryNumericStringTypeRecord() returns error? {
    stream<NumericStringType, sql:Error?> streamData = snowflakeClient->query(`SELECT * FROM TEST_DB.PUBLIC.NUMERIC_TYPES`);
    record {|NumericStringType value;|}? data = check streamData.next();
    check streamData.close();
    NumericStringType? returnData = data?.value;

    test:assertEquals(returnData?.id, "1");
    test:assertEquals(returnData?.int_type, "2147483647");
    test:assertEquals(returnData?.bigint_type, "9223372036854774807");
    test:assertEquals(returnData?.smallint_type, "32767");
    test:assertEquals(returnData?.tinyint_type, "127");
    test:assertEquals(returnData?.bit_type, "true");
    test:assertEquals(returnData?.real_type, "1234.567");
    test:assertFalse(returnData?.decimal_type is ());
    test:assertFalse(returnData?.numeric_type is ());
    test:assertFalse(returnData?.float_type is ());
}

public type CustomType int|decimal|float|boolean;

type NumericCustomType record {
    CustomType id;
    CustomType int_type;
    CustomType bigint_type;
    CustomType smallint_type;
    CustomType tinyint_type;
    CustomType bit_type;
    CustomType decimal_type;
    CustomType numeric_type;
    CustomType float_type;
    CustomType real_type;
};

@test:Config {
    groups: ["query", "query-numeric-params"]
}
function testQueryNumericCustomTypeRecord() returns error? {
    stream<NumericCustomType, sql:Error?> streamData = snowflakeClient->query(`SELECT * FROM TEST_DB.PUBLIC.NUMERIC_TYPES`);
    record {|NumericCustomType value;|}? data = check streamData.next();
    check streamData.close();
    NumericCustomType? returnData = data?.value;

    test:assertEquals(returnData?.id, 1);
    test:assertEquals(returnData?.int_type, 2147483647);
    test:assertEquals(returnData?.bigint_type, 9223372036854774807);
    test:assertEquals(returnData?.smallint_type, 32767);
    test:assertEquals(returnData?.tinyint_type, 127);
    test:assertEquals(returnData?.bit_type, 1);
    test:assertEquals(returnData?.real_type, 1234.567);
    test:assertTrue(returnData?.decimal_type is decimal);
    test:assertTrue(returnData?.numeric_type is decimal);
    test:assertTrue(returnData?.float_type is float);

}
