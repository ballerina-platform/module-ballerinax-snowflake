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
function setupExecuteParamsDB() returns error? {
    _ = check snowflakeClient->execute(`CREATE DATABASE IF NOT EXISTS EXECUTE_PARAMS_DB`);
    _ = check snowflakeClient->execute(`CREATE TEMPORARY TABLE IF NOT EXISTS EXECUTE_PARAMS_DB.PUBLIC.NUMERIC_TYPES (
                                                                                                           row_id INT AUTOINCREMENT,
                                                                                                           int_type INT ,
                                                                                                           bigint_type BIGINT,
                                                                                                           smallint_type SMALLINT ,
                                                                                                           mediumint_type INTEGER ,
                                                                                                           tinyint_type TINYINT,
                                                                                                           bit_type BOOLEAN ,
                                                                                                           decimal_type DECIMAL(10,3) ,
                                                                                                           numeric_type NUMERIC(10,3) ,
                                                                                                           float_type FLOAT ,
                                                                                                           real_type REAL ,
                                                                                                           PRIMARY KEY (row_id)
                                                                                                        )`);
    _ = check snowflakeClient->execute(`INSERT INTO EXECUTE_PARAMS_DB.PUBLIC.NUMERIC_TYPES (row_id, int_type, bigint_type, smallint_type, mediumint_type, tinyint_type, bit_type, decimal_type, numeric_type,
                                                                                        float_type, real_type) VALUES (1, 2147483647, 9223372036854774807, 32767, 8388607, 127, 1, 1234.567, 1234.567, 1234.567,
                                                                                        1234.567)`);
    _ = check snowflakeClient->execute(`INSERT INTO EXECUTE_PARAMS_DB.PUBLIC.NUMERIC_TYPES (row_id, int_type, bigint_type, smallint_type, mediumint_type, tinyint_type, bit_type, decimal_type, numeric_type,
                                                                                        float_type, real_type) VALUES (2, 2147483647, 9223372036854774807, 32767, 8388607, 127, 1, 1234, 1234, 1234,
                                                                                        1234)`);
    _ = check snowflakeClient->execute(`CREATE TEMPORARY TABLE IF NOT EXISTS EXECUTE_PARAMS_DB.PUBLIC.COMPLEX_TYPES (
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
    _ = check snowflakeClient->execute(`INSERT INTO EXECUTE_PARAMS_DB.PUBLIC.COMPLEX_TYPES (row_id, tinyblob_type, blob_type, mediumblob_type, longblob_type, tinytext_type, text_type,
                                        mediumtext_type, longtext_type, binary_type, var_binary_type) VALUES
                                          (1, X'77736F322062616C6C6572696E6120626C6F6220746573742E', X'77736F322062616C6C6572696E6120626C6F6220746573742E',
                                          X'77736F322062616C6C6572696E6120626C6F6220746573742E', X'77736F322062616C6C6572696E6120626C6F6220746573742E',
                                          'very long text', 'very long text','very long text','very long text',
                                          X'77736F322062616C6C6572696E612062696E61727920746573742E', X'77736F322062616C6C6572696E612062696E61727920746573742E')`);
    _ = check snowflakeClient->execute(`INSERT INTO EXECUTE_PARAMS_DB.PUBLIC.COMPLEX_TYPES (row_id, tinyblob_type, blob_type, mediumblob_type, longblob_type, tinytext_type, text_type,
                                        mediumtext_type, longtext_type, binary_type, var_binary_type) VALUES
                                          (2, X'77736F322062616C6C6572696E6120626C6F6220746573742E', X'77736F322062616C6C6572696E6120626C6F6220746573742E',
                                          X'77736F322062616C6C6572696E6120626C6F6220746573742E', X'77736F322062616C6C6572696E6120626C6F6220746573742E',
                                          'very long text', 'very long text','very long text','very long text',
                                          X'77736F322062616C6C6572696E612062696E61727920746573742E', X'77736F322062616C6C6572696E612062696E61727920746573742E')`);
    _ = check snowflakeClient->execute(`INSERT INTO EXECUTE_PARAMS_DB.PUBLIC.COMPLEX_TYPES (row_id, tinyblob_type, blob_type, mediumblob_type, longblob_type, tinytext_type, text_type,
                                        mediumtext_type, longtext_type, binary_type, var_binary_type) VALUES
                                          (3, null, null, null, null, null, null, null, null, null, null)`);
    _ = check snowflakeClient->execute(`CREATE TEMPORARY TABLE IF NOT EXISTS EXECUTE_PARAMS_DB.PUBLIC.DATE_TIME_TYPES (
                                                                                                       row_id         INTEGER NOT NULL,
                                                                                                       date_type      DATE,
                                                                                                       time_type      TIME,
                                                                                                       timestamp_type TIMESTAMP,
                                                                                                       datetime_type  DATETIME,
                                                                                                       PRIMARY KEY (row_id)
                                                                                                     )`);
    _ = check snowflakeClient->execute(`INSERT INTO EXECUTE_PARAMS_DB.PUBLIC.DATE_TIME_TYPES (row_id, date_type, time_type, datetime_type, timestamp_type) VALUES
                                            (1,'2017-02-03', '11:35:45', '2017-02-03 11:53:00', '2017-02-03 11:53:00')`);
    // snowflake does not support enum type. So, we use a varchar type instead.
    _ = check snowflakeClient->execute(`CREATE TEMPORARY TABLE EXECUTE_PARAMS_DB.PUBLIC.ENUM_TABLE (
                                            id integer NOT NULL,
                                            enum_type VARCHAR DEFAULT NULL,
                                            PRIMARY KEY (id)
                                        )`);
    _ = check snowflakeClient->execute(`INSERT INTO EXECUTE_PARAMS_DB.PUBLIC.ENUM_TABLE (id, enum_type) VALUES
                                            (1, 'doctor')`);
    _ = check snowflakeClient->execute(`CREATE TEMPORARY TABLE IF NOT EXISTS EXECUTE_PARAMS_DB.PUBLIC.JSON_TYPES (
                                                                                                   row_id         INTEGER NOT NULL,
                                                                                                   json_type      VARIANT,
                                                                                                   PRIMARY KEY (row_id)
                                                                                                 )`);
    _ = check snowflakeClient->execute(`INSERT INTO EXECUTE_PARAMS_DB.PUBLIC.JSON_TYPES select column1 as row_id, PARSE_JSON(column2) as json_type from values
                                            (1, '{"name":"John", "age":31, "city":"New York"}')`);
    _ = check snowflakeClient->execute(`CREATE TEMPORARY TABLE IF NOT EXISTS EXECUTE_PARAMS_DB.PUBLIC.GEOMETRY_TYPES (
                                                                                                   row_id         INTEGER NOT NULL,
                                                                                                   geometry_type      GEOMETRY,
                                                                                                   PRIMARY KEY (row_id)
                                                                                                 )`);
    _ = check snowflakeClient->execute(`INSERT INTO EXECUTE_PARAMS_DB.PUBLIC.GEOMETRY_TYPES (row_id, geometry_type) VALUES
                                            (1, 'POINT(1 1)')`);
    _ = check snowflakeClient->execute(`CREATE TEMPORARY TABLE IF NOT EXISTS EXECUTE_PARAMS_DB.PUBLIC.DATA_TABLE (
                                                                                                        row_id       INTEGER,
                                                                                                        int_type     INTEGER,
                                                                                                        long_type    BIGINT,
                                                                                                        float_type   FLOAT,
                                                                                                        double_type  DOUBLE,
                                                                                                        boolean_type BOOLEAN,
                                                                                                        string_type  VARCHAR(50),
                                                                                                        decimal_type DECIMAL(20, 2),
                                                                                                        PRIMARY KEY (row_id)
                                                                                                      )`);
    _ = check snowflakeClient->execute(`INSERT INTO EXECUTE_PARAMS_DB.PUBLIC.DATA_TABLE (row_id, int_type, long_type, float_type, double_type, boolean_type, string_type, decimal_type) VALUES
                                            (1, 1, 9223372036854774807, 123.34, 2139095039, TRUE, 'Hello', 23.45)`);
    _ = check snowflakeClient->execute(`INSERT INTO EXECUTE_PARAMS_DB.PUBLIC.DATA_TABLE (row_id) VALUES (2)`);
    _ = check snowflakeClient->execute(`INSERT INTO EXECUTE_PARAMS_DB.PUBLIC.DATA_TABLE (row_id, int_type, long_type, float_type, double_type, boolean_type, string_type, decimal_type) VALUES
                                            (3, 1, 9372036854774807, 124.34, 29095039, false, '1', 25.45)`);
}

@test:Config {
    groups: ["execute", "execute-params"]
}
function insertIntoDataTable() returns error? {
    int rowId = 4;
    int intType = 1;
    int longType = 9223372036854774807;
    float floatType = 123.34;
    int doubleType = 2139095039;
    boolean boolType = true;
    string stringType = "Hello";
    decimal decimalType = 23.45;

    sql:ParameterizedQuery sqlQuery =
      `INSERT INTO EXECUTE_PARAMS_DB.PUBLIC.DATA_TABLE (row_id, int_type, long_type, float_type, double_type, boolean_type, string_type, decimal_type)
        VALUES(${rowId}, ${intType}, ${longType}, ${floatType}, ${doubleType}, ${boolType}, ${stringType}, ${decimalType})`;
    sql:ExecutionResult result = check snowflakeClient->execute(sqlQuery);

    int insertedId = check snowflakeClient->queryRow(`select max(row_id) from EXECUTE_PARAMS_DB.PUBLIC.DATA_TABLE AT(statement=>last_query_id())`);

    validateResult(result, 1, insertedId);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: [insertIntoDataTable]
}
function insertIntoDataTable2() returns error? {
    int rowId = 5;
    sql:ParameterizedQuery sqlQuery = `INSERT INTO EXECUTE_PARAMS_DB.PUBLIC.DATA_TABLE (row_id) VALUES(${rowId})`;
    sql:ExecutionResult result = check snowflakeClient->execute(sqlQuery);
    int insertedId = check snowflakeClient->queryRow(`select max(row_id) from EXECUTE_PARAMS_DB.PUBLIC.DATA_TABLE AT(statement=>last_query_id())`);
    validateResult(result, 1, insertedId);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: [insertIntoDataTable2]
}
function insertIntoDataTable3() returns error? {
    int rowId = 6;
    int intType = 1;
    int longType = 9223372036854774807;
    float floatType = 123.34;
    int doubleType = 2139095039;
    boolean boolType = false;
    string stringType = "1";
    decimal decimalType = 23.45;

    sql:ParameterizedQuery sqlQuery =
      `INSERT INTO EXECUTE_PARAMS_DB.PUBLIC.DATA_TABLE (row_id, int_type, long_type, float_type, double_type, boolean_type, string_type, decimal_type)
        VALUES(${rowId}, ${intType}, ${longType}, ${floatType}, ${doubleType}, ${boolType}, ${stringType}, ${decimalType})`;

    sql:ExecutionResult result = check snowflakeClient->execute(sqlQuery);
    int insertedId = check snowflakeClient->queryRow(`select max(row_id) from EXECUTE_PARAMS_DB.PUBLIC.DATA_TABLE AT(statement=>last_query_id())`);
    validateResult(result, 1, insertedId);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: [insertIntoDataTable3]
}
function insertIntoDataTable4() returns error? {
    sql:IntegerValue rowId = new (7);
    sql:IntegerValue intType = new (2);
    sql:BigIntValue longType = new (9372036854774807);
    sql:FloatValue floatType = new (124.34);
    sql:DoubleValue doubleType = new (29095039);
    sql:BooleanValue boolType = new (false);
    sql:VarcharValue stringType = new ("stringValue");
    decimal decimalVal = 25.45;
    sql:DecimalValue decimalType = new (decimalVal);

    sql:ParameterizedQuery sqlQuery =
      `INSERT INTO EXECUTE_PARAMS_DB.PUBLIC.DATA_TABLE (row_id, int_type, long_type, float_type, double_type, boolean_type, string_type, decimal_type)
        VALUES(${rowId}, ${intType}, ${longType}, ${floatType}, ${doubleType}, ${boolType}, ${stringType}, ${decimalType})`;
    sql:ExecutionResult result = check snowflakeClient->execute(sqlQuery);
    int insertedId = check snowflakeClient->queryRow(`select max(row_id) from EXECUTE_PARAMS_DB.PUBLIC.DATA_TABLE AT(statement=>last_query_id())`);
    validateResult(result, 1, insertedId);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: [insertIntoDataTable4]
}
function deleteDataTable1() returns error? {
    int rowId = 1;
    int intType = 1;
    int longType = 9223372036854774807;
    int doubleType = 2139095039;
    boolean boolType = true;
    string stringType = "Hello";
    decimal decimalType = 23.45;

    sql:ParameterizedQuery sqlQuery =
            `DELETE FROM EXECUTE_PARAMS_DB.PUBLIC.DATA_TABLE where row_id=${rowId} AND int_type=${intType} AND long_type=${longType}
              AND double_type=${doubleType} AND boolean_type=${boolType}
              AND string_type=${stringType} AND decimal_type=${decimalType}`;
    sql:ExecutionResult result = check snowflakeClient->execute(sqlQuery);
    validateResult(result, 1);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: [deleteDataTable1]
}
function deleteDataTable2() returns error? {
    int rowId = 2;
    sql:ParameterizedQuery sqlQuery = `DELETE FROM EXECUTE_PARAMS_DB.PUBLIC.DATA_TABLE where row_id = ${rowId}`;
    sql:ExecutionResult result = check snowflakeClient->execute(sqlQuery);
    validateResult(result, 1);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: [deleteDataTable2]
}
function deleteDataTable3() returns error? {
    sql:IntegerValue rowId = new (3);
    sql:IntegerValue intType = new (1);
    sql:BigIntValue longType = new (9372036854774807);
    sql:DoubleValue doubleType = new (29095039);
    sql:BooleanValue boolType = new (false);
    sql:VarcharValue stringType = new ("1");
    decimal decimalVal = 25.45;
    sql:DecimalValue decimalType = new (decimalVal);

    sql:ParameterizedQuery sqlQuery =
            `DELETE FROM EXECUTE_PARAMS_DB.PUBLIC.DATA_TABLE where row_id=${rowId} AND int_type=${intType} AND long_type=${longType}
              AND double_type=${doubleType} AND boolean_type=${boolType}
              AND string_type=${stringType} AND decimal_type=${decimalType}`;
    sql:ExecutionResult result = check snowflakeClient->execute(sqlQuery);
    validateResult(result, 1);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: [deleteDataTable3]
}
function insertIntoComplexTable() returns error? {
    ComplexTypes value = check snowflakeClient->queryRow(`Select * from EXECUTE_PARAMS_DB.PUBLIC.COMPLEX_TYPES where row_id = 1`);
    byte[] binaryData = value.blob_type;
    int rowId = 5;
    string stringType = "very long text";
    sql:ParameterizedQuery sqlQuery =
        `INSERT INTO EXECUTE_PARAMS_DB.PUBLIC.COMPLEX_TYPES (row_id, blob_type, text_type, binary_type, var_binary_type) VALUES (
        ${rowId}, ${binaryData}, ${stringType}, ${binaryData}, ${binaryData})`;
    sql:ExecutionResult result = check snowflakeClient->execute(sqlQuery);
    int insertedId = check snowflakeClient->queryRow(`select max(row_id) from EXECUTE_PARAMS_DB.PUBLIC.COMPLEX_TYPES AT(statement=>last_query_id())`);
    validateResult(result, 1, insertedId);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: [insertIntoComplexTable],
    enable: false
}
function insertIntoComplexTable2() returns error? {
    io:ReadableByteChannel blobChannel = check getBlobColumnChannel();
    io:ReadableCharacterChannel clobChannel = check getClobColumnChannel();
    io:ReadableByteChannel byteChannel = check getByteColumnChannel();

    sql:BlobValue blobType = new (blobChannel);
    sql:TextValue textType = new (clobChannel);
    sql:BlobValue binaryType = new (byteChannel);
    int rowId = 6;

    sql:ParameterizedQuery sqlQuery =
        `INSERT INTO EXECUTE_PARAMS_DB.PUBLIC.COMPLEX_TYPES (row_id, blob_type, text_type, binary_type, var_binary_type) VALUES (
        ${rowId}, ${blobType}, ${textType}, ${binaryType}, ${binaryType})`;
    sql:ExecutionResult result = check snowflakeClient->execute(sqlQuery);
    int insertedId = check snowflakeClient->queryRow(`select max(row_id) from EXECUTE_PARAMS_DB.PUBLIC.COMPLEX_TYPES AT(statement=>last_query_id())`);
    validateResult(result, 1, insertedId);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: [insertIntoComplexTable]
}
function insertIntoComplexTable3() returns error? {
    int rowId = 7;
    var nilType = ();
    sql:ParameterizedQuery sqlQuery =
            `INSERT INTO EXECUTE_PARAMS_DB.PUBLIC.COMPLEX_TYPES (row_id, blob_type, text_type, binary_type, var_binary_type) VALUES (
            ${rowId}, ${nilType}, ${nilType}, ${nilType}, ${nilType})`;
    sql:ExecutionResult result = check snowflakeClient->execute(sqlQuery);
    int insertedId = check snowflakeClient->queryRow(`select max(row_id) from EXECUTE_PARAMS_DB.PUBLIC.COMPLEX_TYPES AT(statement=>last_query_id())`);
    validateResult(result, 1, insertedId);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: [insertIntoComplexTable3]
}
function deleteComplexTable() returns error? {
    ComplexTypes value = check snowflakeClient->queryRow(`Select * from EXECUTE_PARAMS_DB.PUBLIC.COMPLEX_TYPES where row_id = 1`);
    byte[] binaryData = value.blob_type;

    int rowId = 2;
    sql:ParameterizedQuery sqlQuery =
            `DELETE FROM EXECUTE_PARAMS_DB.PUBLIC.COMPLEX_TYPES where row_id = ${rowId} AND blob_type= ${binaryData}`;
    sql:ExecutionResult result = check snowflakeClient->execute(sqlQuery);
    validateResult(result, 1);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: [deleteComplexTable]
}
function deleteComplexTable2() returns error? {
    sql:BlobValue blobType = new ();
    sql:TextValue textType = new ();

    int rowId = 4;
    sql:ParameterizedQuery sqlQuery =
            `DELETE FROM EXECUTE_PARAMS_DB.PUBLIC.COMPLEX_TYPES where row_id = ${rowId} AND blob_type= ${blobType} AND text_type=${textType}`;
    sql:ExecutionResult result = check snowflakeClient->execute(sqlQuery);
    validateResult(result, 0);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: [deleteComplexTable2]
}
function insertIntoNumericTable() returns error? {
    sql:BitValue bitType = new (1);
    int rowId = 3;
    int intType = 2147483647;
    int bigIntType = 9223372036854774807;
    int smallIntType = 32767;
    int tinyIntType = 127;
    decimal decimalType = 1234.567;

    sql:ParameterizedQuery sqlQuery =
        `INSERT INTO EXECUTE_PARAMS_DB.PUBLIC.NUMERIC_TYPES (row_id, int_type, bigint_type, smallint_type, tinyint_type, bit_type, decimal_type,
        numeric_type, float_type, real_type) VALUES(${rowId},${intType},${bigIntType},${smallIntType},${tinyIntType},
        ${bitType},${decimalType},${decimalType},${decimalType},${decimalType})`;
    sql:ExecutionResult result = check snowflakeClient->execute(sqlQuery);
    int insertedId = check snowflakeClient->queryRow(`select max(row_id) from EXECUTE_PARAMS_DB.PUBLIC.NUMERIC_TYPES AT(statement=>last_query_id())`);
    validateResult(result, 1, insertedId);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: [insertIntoNumericTable]
}
function insertIntoNumericTable2() returns error? {
    int rowId = 4;
    var nilType = ();
    sql:ParameterizedQuery sqlQuery =
            `INSERT INTO EXECUTE_PARAMS_DB.PUBLIC.NUMERIC_TYPES (row_id, int_type, bigint_type, smallint_type, tinyint_type, bit_type, decimal_type,
            numeric_type, float_type, real_type) VALUES(${rowId},${nilType},${nilType},${nilType},${nilType},
            ${nilType},${nilType},${nilType},${nilType},${nilType})`;
    sql:ExecutionResult result = check snowflakeClient->execute(sqlQuery);
    int insertedId = check snowflakeClient->queryRow(`select max(row_id) from EXECUTE_PARAMS_DB.PUBLIC.NUMERIC_TYPES AT(statement=>last_query_id())`);
    validateResult(result, 1, insertedId);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: [insertIntoNumericTable2]
}
function insertIntoNumericTable3() returns error? {
    sql:IntegerValue id = new (5);
    sql:IntegerValue intType = new (2147483647);
    sql:BigIntValue bigIntType = new (9223372036854774807);
    sql:SmallIntValue smallIntType = new (32767);
    sql:SmallIntValue tinyIntType = new (127);
    sql:BitValue bitType = new (1);
    decimal decimalVal = 1234.567;
    sql:DecimalValue decimalType = new (decimalVal);
    sql:NumericValue numericType = new (1234.567);
    sql:FloatValue floatType = new (1234.567);
    sql:RealValue realType = new (1234.567);

    sql:ParameterizedQuery sqlQuery =
        `INSERT INTO EXECUTE_PARAMS_DB.PUBLIC.NUMERIC_TYPES (row_id, int_type, bigint_type, smallint_type, tinyint_type, bit_type, decimal_type,
        numeric_type, float_type, real_type) VALUES(${id},${intType},${bigIntType},${smallIntType},${tinyIntType},
        ${bitType},${decimalType},${numericType},${floatType},${realType})`;
    sql:ExecutionResult result = check snowflakeClient->execute(sqlQuery);
    int insertedId = check snowflakeClient->queryRow(`select max(row_id) from EXECUTE_PARAMS_DB.PUBLIC.NUMERIC_TYPES AT(statement=>last_query_id())`);
    validateResult(result, 1, insertedId);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: [insertIntoNumericTable3]
}
function insertIntoDateTimeTable() returns error? {
    int rowId = 2;
    string dateType = "2017-02-03";
    string timeType = "11:35:45";
    string dateTimeType = "2017-02-03 11:53:00";
    string timeStampType = "2017-02-03 11:53:00";

    sql:ParameterizedQuery sqlQuery =
        `INSERT INTO EXECUTE_PARAMS_DB.PUBLIC.DATE_TIME_TYPES (row_id, date_type, time_type, datetime_type, timestamp_type)
        VALUES(${rowId}, ${dateType}, ${timeType}, ${dateTimeType}, ${timeStampType})`;
    sql:ExecutionResult result = check snowflakeClient->execute(sqlQuery);
    int insertedId = check snowflakeClient->queryRow(`select max(row_id) from EXECUTE_PARAMS_DB.PUBLIC.DATE_TIME_TYPES AT(statement=>last_query_id())`);
    validateResult(result, 1, insertedId);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: [insertIntoDateTimeTable]
}
function insertIntoDateTimeTable2() returns error? {
    sql:DateValue dateVal = new ("2017-02-03");
    sql:TimeValue timeVal = new ("11:35:45");
    sql:DateTimeValue dateTimeVal = new ("2017-02-03 11:53:00");
    sql:TimestampValue timestampVal = new ("2017-02-03 11:53:00");
    int rowId = 3;

    sql:ParameterizedQuery sqlQuery =
            `INSERT INTO EXECUTE_PARAMS_DB.PUBLIC.DATE_TIME_TYPES (row_id, date_type, time_type, datetime_type, timestamp_type)
            VALUES(${rowId}, ${dateVal}, ${timeVal}, ${dateTimeVal}, ${timestampVal})`;

    sql:ExecutionResult result = check snowflakeClient->execute(sqlQuery);
    int insertedId = check snowflakeClient->queryRow(`select max(row_id) from EXECUTE_PARAMS_DB.PUBLIC.DATE_TIME_TYPES AT(statement=>last_query_id())`);
    validateResult(result, 1, insertedId);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: [insertIntoDateTimeTable2]
}
function insertIntoDateTimeTable3() returns error? {
    sql:DateValue dateVal = new ();
    sql:TimeValue timeVal = new ();
    sql:DateTimeValue dateTimeVal = new ();
    sql:TimestampValue timestampVal = new ();
    int rowId = 4;

    sql:ParameterizedQuery sqlQuery =
                `INSERT INTO EXECUTE_PARAMS_DB.PUBLIC.DATE_TIME_TYPES (row_id, date_type, time_type, datetime_type, timestamp_type)
                VALUES(${rowId}, ${dateVal}, ${timeVal}, ${dateTimeVal}, ${timestampVal})`;
    sql:ExecutionResult result = check snowflakeClient->execute(sqlQuery);
    int insertedId = check snowflakeClient->queryRow(`select max(row_id) from EXECUTE_PARAMS_DB.PUBLIC.DATE_TIME_TYPES AT(statement=>last_query_id())`);
    validateResult(result, 1, insertedId);
}

@test:Config {
    groups: ["execute", "execute-params"],
    dependsOn: [insertIntoDateTimeTable3]
}
function insertIntoDateTimeTable4() returns error? {
    int rowId = 5;
    var nilType = ();

    sql:ParameterizedQuery sqlQuery =
            `INSERT INTO EXECUTE_PARAMS_DB.PUBLIC.DATE_TIME_TYPES (row_id, date_type, time_type, datetime_type, timestamp_type)
            VALUES(${rowId}, ${nilType}, ${nilType}, ${nilType}, ${nilType})`;
    sql:ExecutionResult result = check snowflakeClient->execute(sqlQuery);
    int insertedId = check snowflakeClient->queryRow(`select max(row_id) from EXECUTE_PARAMS_DB.PUBLIC.DATE_TIME_TYPES AT(statement=>last_query_id())`);
    validateResult(result, 1, insertedId);
}

@test:Config {
    groups: ["execute", "execute-params"]
}
function insertIntoEnumTable() returns error? {
    int rowId = 2;
    string enumType = "doctor";

    sql:ParameterizedQuery sqlQuery =
            `INSERT INTO EXECUTE_PARAMS_DB.PUBLIC.ENUM_TABLE (id, enum_type) VALUES(${rowId}, ${enumType})`;
    sql:ExecutionResult result = check snowflakeClient->execute(sqlQuery);
    int insertedId = check snowflakeClient->queryRow(`select id from EXECUTE_PARAMS_DB.PUBLIC.ENUM_TABLE where id = ${rowId}`);
    validateResult(result, 1, insertedId);
}

@test:Config {
    groups: ["execute", "execute-params"]
}
function insertIntoEnumTable2() returns error? {
    int rowId = 3;
    var nilType = ();

    sql:ParameterizedQuery sqlQuery =
            `INSERT INTO EXECUTE_PARAMS_DB.PUBLIC.ENUM_TABLE (id, enum_type) VALUES(${rowId}, ${nilType})`;
    sql:ExecutionResult result = check snowflakeClient->execute(sqlQuery);
    int insertedId = check snowflakeClient->queryRow(`select id from EXECUTE_PARAMS_DB.PUBLIC.ENUM_TABLE where id = ${rowId}`);
    validateResult(result, 1, insertedId);
}

@test:Config {
    groups: ["execute", "execute-params"]
}
function insertIntoJsonTable() returns error? {
    int rowId = 2;
    string jsonType = string `{"name":"John", "age":31, "city":"New York"}`;
    sql:ParameterizedQuery sqlQuery =
            `INSERT INTO EXECUTE_PARAMS_DB.PUBLIC.JSON_TYPES select column1 as row_id, PARSE_JSON(column2) as json_type from values(${rowId}, ${jsonType})`;
    sql:ExecutionResult result = check snowflakeClient->execute(sqlQuery);
    int insertedId = check snowflakeClient->queryRow(`select row_id from EXECUTE_PARAMS_DB.PUBLIC.JSON_TYPES where row_id = ${rowId}`);
    validateResult(result, 1, insertedId);
}

@test:Config {
    groups: ["execute", "execute-params"]
}
function insertIntoJsonTable2() returns error? {
    int rowId = 3;
    var nilType = ();
    sql:ParameterizedQuery sqlQuery =
            `INSERT INTO EXECUTE_PARAMS_DB.PUBLIC.JSON_TYPES select column1 as row_id, PARSE_JSON(column2) as json_type from values(${rowId}, ${nilType})`;
    sql:ExecutionResult result = check snowflakeClient->execute(sqlQuery);
    int insertedId = check snowflakeClient->queryRow(`select row_id from EXECUTE_PARAMS_DB.PUBLIC.JSON_TYPES where row_id = ${rowId}`);
    validateResult(result, 1, insertedId);
}

// test case for geometry type
@test:Config {
    groups: ["execute", "execute-params"]
}
function insertIntoGeometryTable() returns error? {
    int rowId = 2;
    string geometryType = "POINT(1 1)";
    sql:ParameterizedQuery sqlQuery =
            `INSERT INTO EXECUTE_PARAMS_DB.PUBLIC.GEOMETRY_TYPES (row_id, geometry_type) VALUES(${rowId}, ${geometryType})`;
    sql:ExecutionResult result = check snowflakeClient->execute(sqlQuery);
    int insertedId = check snowflakeClient->queryRow(`select row_id from EXECUTE_PARAMS_DB.PUBLIC.GEOMETRY_TYPES where row_id = ${rowId}`);
    validateResult(result, 1, insertedId);
}

@test:Config {
    groups: ["execute", "execute-params"]
}
function insertIntoGeometryTable2() returns error? {
    int rowId = 3;
    var nilType = ();
    sql:ParameterizedQuery sqlQuery =
            `INSERT INTO EXECUTE_PARAMS_DB.PUBLIC.GEOMETRY_TYPES (row_id, geometry_type) VALUES(${rowId}, ${nilType})`;
    sql:ExecutionResult result = check snowflakeClient->execute(sqlQuery);
    int insertedId = check snowflakeClient->queryRow(`select row_id from EXECUTE_PARAMS_DB.PUBLIC.GEOMETRY_TYPES where row_id = ${rowId}`);
    validateResult(result, 1, insertedId);
}

isolated function validateResult(sql:ExecutionResult result, int rowCount, int? lastId = ()) {
    test:assertExactEquals(result.affectedRowCount, rowCount, "Affected row count is different.");
    if lastId is int {
        test:assertTrue(lastId > 1, "Last Insert Id is nil.");
    }
}

@test:AfterSuite
function cleanUpExecuteParamsDB() returns error? {
    _ = check snowflakeClient->execute(`DROP DATABASE IF EXISTS EXECUTE_PARAMS_DB`);
}