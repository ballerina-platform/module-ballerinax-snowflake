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

import ballerina/io;
import ballerina/sql;
import ballerina/test;
import ballerinax/snowflake.driver as _;

@test:BeforeSuite
function setupComplexTypesTable() returns error? {
    _ = check snowflakeClient->execute(`CREATE DATABASE IF NOT EXISTS TEST_DB`);
    _ = check snowflakeClient->execute(`CREATE TEMPORARY TABLE IF NOT EXISTS TEST_DB.PUBLIC.COMPLEX_TYPES (
                                                                 row_id         INTEGER NOT NULL,
                                                                 tinyblob_type     BINARY(255),
                                                                 blob_type         BINARY,
                                                                 mediumblob_type   BINARY,
                                                                 longblob_type     BINARY,
                                                                 tinytext_type   STRING,
                                                                 text_type       STRING,
                                                                 mediumtext_type TEXT,
                                                                 longtext_type   TEXT,
                                                                 binary_type  BINARY(27),
                                                                 var_binary_type VARBINARY(27),
                                                                 PRIMARY KEY (row_id)
                                                               )`);
    _ = check snowflakeClient->execute(`INSERT INTO TEST_DB.PUBLIC.COMPLEX_TYPES (row_id, tinyblob_type, blob_type, mediumblob_type, longblob_type, tinytext_type, text_type,
                                        mediumtext_type, longtext_type, binary_type, var_binary_type) VALUES
                                          (1, X'77736F322062616C6C6572696E6120626C6F6220746573742E', X'77736F322062616C6C6572696E6120626C6F6220746573742E',
                                          X'77736F322062616C6C6572696E6120626C6F6220746573742E', X'77736F322062616C6C6572696E6120626C6F6220746573742E',
                                          'very long text', 'very long text','very long text','very long text',
                                          X'77736F322062616C6C6572696E612062696E61727920746573742E', X'77736F322062616C6C6572696E612062696E61727920746573742E')`);
    _ = check snowflakeClient->execute(`INSERT INTO TEST_DB.PUBLIC.COMPLEX_TYPES (row_id, tinyblob_type, blob_type, mediumblob_type, longblob_type, tinytext_type, text_type,
                                        mediumtext_type, longtext_type, binary_type, var_binary_type) VALUES
                                          (2, null, null, null, null, null, null, null, null, null, null)`);
    return ();
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryByteArrayParam() returns error? {
    sql:ParameterizedQuery sqlQuery = `Select * from TEST_DB.PUBLIC.COMPLEX_TYPES where row_id = 1`;
    stream<ComplexTypes, error?> resultStream = snowflakeClient->query(sqlQuery);
    ComplexTypes[] data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    byte[] binaryData = data[0].binary_type;

    sqlQuery = `SELECT * from TEST_DB.PUBLIC.COMPLEX_TYPES WHERE binary_type = ${binaryData}`;
    resultStream = snowflakeClient->query(sqlQuery);
    data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateComplexTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeBinaryByteParam() returns error? {
    sql:ParameterizedQuery sqlQuery = `Select * from TEST_DB.PUBLIC.COMPLEX_TYPES where row_id = 1`;
    stream<ComplexTypes, error?> resultStream = snowflakeClient->query(sqlQuery);
    ComplexTypes[] data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    byte[] binaryData = data[0].binary_type;

    sql:BinaryValue typeVal = new (binaryData);
    sqlQuery = `SELECT * from TEST_DB.PUBLIC.COMPLEX_TYPES WHERE binary_type = ${typeVal}`;
    resultStream = snowflakeClient->query(sqlQuery);
    data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateComplexTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-simple-params"],
    enable: false
}
function queryTypeBinaryReadableByteChannelParam() returns error? {
    io:ReadableByteChannel byteChannel = check getByteColumnChannel();
    sql:BinaryValue typeVal = new (byteChannel);
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB.PUBLIC.COMPLEX_TYPES WHERE binary_type = ${typeVal}`;
    stream<ComplexTypes, error?> resultStream = snowflakeClient->query(sqlQuery);
    ComplexTypes[] data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateComplexTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-simple-params"],
    enable: false
}
function queryTypeVarBinaryReadableByteChannelParam() returns error? {
    io:ReadableByteChannel byteChannel = check getByteColumnChannel();
    sql:VarBinaryValue typeVal = new (byteChannel);
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB.PUBLIC.COMPLEX_TYPES WHERE binary_type = ${typeVal}`;
    stream<ComplexTypes, error?> resultStream = snowflakeClient->query(sqlQuery);
    ComplexTypes[] data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateComplexTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeTinyBlobByteParam() returns error? {
    sql:ParameterizedQuery sqlQuery = `Select * from TEST_DB.PUBLIC.COMPLEX_TYPES where row_id = 1`;
    stream<ComplexTypes, error?> resultStream = snowflakeClient->query(sqlQuery);
    ComplexTypes[] data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    byte[] binaryData = data[0].tinyblob_type;

    sql:BinaryValue typeVal = new (binaryData);
    sqlQuery = `SELECT * from TEST_DB.PUBLIC.COMPLEX_TYPES WHERE tinyblob_type = ${typeVal}`;
    resultStream = snowflakeClient->query(sqlQuery);
    data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateComplexTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeBlobByteParam() returns error? {
    sql:ParameterizedQuery sqlQuery = `Select * from TEST_DB.PUBLIC.COMPLEX_TYPES where row_id = 1`;
    stream<ComplexTypes, error?> resultStream = snowflakeClient->query(sqlQuery);
    ComplexTypes[] data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    byte[] binaryData = data[0].blob_type;

    sql:BlobValue typeVal = new (binaryData);
    sqlQuery = `SELECT * from TEST_DB.PUBLIC.COMPLEX_TYPES WHERE blob_type = ${typeVal}`;
    resultStream = snowflakeClient->query(sqlQuery);
    data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateComplexTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeMediumBlobByteParam() returns error? {
    sql:ParameterizedQuery sqlQuery = `Select * from TEST_DB.PUBLIC.COMPLEX_TYPES where row_id = 1`;
    stream<ComplexTypes, error?> resultStream = snowflakeClient->query(sqlQuery);
    ComplexTypes[] data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    byte[] binaryData = data[0].mediumblob_type;

    sql:BlobValue typeVal = new (binaryData);
    sqlQuery = `SELECT * from TEST_DB.PUBLIC.COMPLEX_TYPES WHERE mediumblob_type = ${typeVal}`;
    resultStream = snowflakeClient->query(sqlQuery);
    data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateComplexTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeLongBlobByteParam() returns error? {
    sql:ParameterizedQuery sqlQuery = `Select * from TEST_DB.PUBLIC.COMPLEX_TYPES where row_id = 1`;
    stream<ComplexTypes, error?> resultStream = snowflakeClient->query(sqlQuery);
    ComplexTypes[] data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    byte[] binaryData = data[0].longblob_type;

    sql:BlobValue typeVal = new (binaryData);
    sqlQuery = `SELECT * from TEST_DB.PUBLIC.COMPLEX_TYPES WHERE longblob_type = ${typeVal}`;
    resultStream = snowflakeClient->query(sqlQuery);
    data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateComplexTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-simple-params"],
    enable: false
}
function queryTypeBlobReadableByteChannelParam() returns error? {
    io:ReadableByteChannel byteChannel = check getBlobColumnChannel();
    sql:BlobValue typeVal = new (byteChannel);
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB.PUBLIC.COMPLEX_TYPES WHERE blob_type = ${typeVal}`;
    stream<ComplexTypes, error?> resultStream = snowflakeClient->query(sqlQuery);
    ComplexTypes[] data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateComplexTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeTinyTextStringParam() returns error? {
    sql:TextValue typeVal = new ("very long text");
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB.PUBLIC.COMPLEX_TYPES WHERE tinytext_type = ${typeVal}`;
    stream<ComplexTypes, error?> resultStream = snowflakeClient->query(sqlQuery);
    ComplexTypes[] data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateComplexTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeTextStringParam() returns error? {
    sql:TextValue typeVal = new ("very long text");
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB.PUBLIC.COMPLEX_TYPES WHERE text_type = ${typeVal}`;
    stream<ComplexTypes, error?> resultStream = snowflakeClient->query(sqlQuery);
    ComplexTypes[] data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateComplexTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeMediumTextStringParam() returns error? {
    sql:TextValue typeVal = new ("very long text");
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB.PUBLIC.COMPLEX_TYPES WHERE mediumtext_type = ${typeVal}`;
    stream<ComplexTypes, error?> resultStream = snowflakeClient->query(sqlQuery);
    ComplexTypes[] data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateComplexTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-simple-params"]
}
function queryTypeLongTextStringParam() returns error? {
    sql:TextValue typeVal = new ("very long text");
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB.PUBLIC.COMPLEX_TYPES WHERE longtext_type = ${typeVal}`;
    stream<ComplexTypes, error?> resultStream = snowflakeClient->query(sqlQuery);
    ComplexTypes[] data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateComplexTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-simple-params"],
    enable: false
}
function queryTypeTextReadableCharChannelParam() returns error? {
    io:ReadableCharacterChannel clobChannel = check getTextColumnChannel();
    sql:ClobValue typeVal = new (clobChannel);
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB.PUBLIC.COMPLEX_TYPES WHERE text_type = ${typeVal}`;
    stream<ComplexTypes, error?> resultStream = snowflakeClient->query(sqlQuery);
    ComplexTypes[] data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateComplexTableResult(data[0]);
}

@test:Config {
    groups: ["query", "query-simple-params"],
    enable: false
}
function queryTypeNTextReadableCharChannelParam() returns error? {
    io:ReadableCharacterChannel clobChannel = check getTextColumnChannel();
    sql:NClobValue typeVal = new (clobChannel);
    sql:ParameterizedQuery sqlQuery = `SELECT * from TEST_DB.PUBLIC.COMPLEX_TYPES WHERE text_type = ${typeVal}`;
    stream<ComplexTypes, error?> resultStream = snowflakeClient->query(sqlQuery);
    ComplexTypes[] data = check from var result in resultStream
        select result;
    test:assertEquals(data.length(), 1, "Invalid number of records returned");
    validateComplexTableResult(data[0]);
}

type ComplexTypes record {
    int row_id;
    byte[] tinyblob_type;
    byte[] blob_type;
    byte[] mediumblob_type;
    byte[] longblob_type;
    string tinytext_type;
    string text_type;
    string mediumtext_type;
    string longtext_type;
    byte[] binary_type;
    byte[] var_binary_type;
};

isolated function validateComplexTableResult(ComplexTypes returnData) {
    test:assertEquals(returnData.length(), 11);
    test:assertEquals(returnData.row_id, 1);
    test:assertEquals(returnData.text_type, "very long text");
}
