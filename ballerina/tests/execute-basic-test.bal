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
function setupExecuteTable() returns error? {
    _ = check snowflakeClient->execute(`CREATE DATABASE IF NOT EXISTS EXECUTE_DB`);
    _ = check snowflakeClient->execute(`CREATE TEMPORARY TABLE IF NOT EXISTS EXECUTE_DB.PUBLIC.NUMERIC_TYPES (
                                                                                                    id INT AUTOINCREMENT,
                                                                                                    int_type INT,
                                                                                                    bigint_type BIGINT,
                                                                                                    smallint_type SMALLINT,
                                                                                                    tinyint_type TINYINT,
                                                                                                    bit_type BOOLEAN,
                                                                                                    decimal_type DECIMAL(10,2),
                                                                                                    numeric_type NUMERIC(10,2),
                                                                                                    float_type FLOAT,
                                                                                                    real_type REAL,
                                                                                                    PRIMARY KEY (id)
                                                                                                 )`);
    _ = check snowflakeClient->execute(`INSERT INTO EXECUTE_DB.PUBLIC.NUMERIC_TYPES (int_type) VALUES (10)`);
    _ = check snowflakeClient->execute(`CREATE TEMPORARY TABLE IF NOT EXISTS EXECUTE_DB.PUBLIC.STRING_TYPES (
                                                                                                    id INT,
                                                                                                    varchar_type VARCHAR(255),
                                                                                                    charmax_type CHAR(10),
                                                                                                    char_type CHAR,
                                                                                                    charactermax_type CHARACTER(10),
                                                                                                    character_type CHARACTER,
                                                                                                    nvarcharmax_type NVARCHAR(255),
                                                                                                    longvarchar_type VARCHAR(511),
                                                                                                    clob_type TEXT,
                                                                                                    PRIMARY KEY (id)
                                                                                                 )`);
}

@test:Config {
    groups: ["execute", "execute-basic"]
}
function testCreateTable() returns error? {
    sql:ExecutionResult result = check snowflakeClient->execute(`CREATE TABLE EXECUTE_DB.PUBLIC.CREATE_TABLE(studentID int, LastName
         varchar(255))`);
    test:assertExactEquals(result.affectedRowCount, 0, "Affected row count is different.");
    test:assertExactEquals(result.lastInsertId, (), "Last Insert Id is not nil.");
}

@test:Config {
    groups: ["execute", "execute-basic"],
    dependsOn: [testCreateTable]
}
function testInsertTable() returns error? {
    sql:ExecutionResult result = check snowflakeClient->execute(`Insert into EXECUTE_DB.PUBLIC.NUMERIC_TYPES (int_type) values (20)`);

    test:assertExactEquals(result.affectedRowCount, 1, "Affected row count is different.");
}

@test:Config {
    groups: ["execute", "execute-basic"],
    dependsOn: [testInsertTable]
}
function testInsertTableWithoutGeneratedKeys() returns error? {
    sql:ExecutionResult result = check snowflakeClient->execute(`Insert into EXECUTE_DB.PUBLIC.STRING_TYPES (id, varchar_type)
         values (20, 'test')`);
    test:assertExactEquals(result.affectedRowCount, 1, "Affected row count is different.");
    test:assertEquals(result.lastInsertId, (), "Last Insert Id is nil.");
}

@test:Config {
    groups: ["execute", "execute-basic"],
    dependsOn: [testInsertTableWithoutGeneratedKeys]
}
function testInsertTableWithGeneratedKeys() returns error? {
    sql:ExecutionResult result = check snowflakeClient->execute(`insert into EXECUTE_DB.PUBLIC.NUMERIC_TYPES (int_type) values (21)`);
    test:assertExactEquals(result.affectedRowCount, 1, "Affected row count is different.");

    int insertedId = check snowflakeClient->queryRow(`select max(id) from EXECUTE_DB.PUBLIC.NUMERIC_TYPES AT(statement=>last_query_id())`);
    test:assertTrue(insertedId > 1, "Last Insert Id is nil.");
}

type NumericType record {
    int id;
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
    groups: ["execute", "execute-basic"],
    dependsOn: [testInsertTableWithGeneratedKeys]
}
function testInsertAndSelectTableWithGeneratedKeys() returns error? {
    sql:ExecutionResult result = check snowflakeClient->execute(`insert into EXECUTE_DB.PUBLIC.NUMERIC_TYPES (int_type) values (31)`);

    test:assertExactEquals(result.affectedRowCount, 1, "Affected row count is different.");

    int insertedId = check snowflakeClient->queryRow(`select max(id) from EXECUTE_DB.PUBLIC.NUMERIC_TYPES AT(statement=>last_query_id())`);
    sql:ParameterizedQuery query = `SELECT * from EXECUTE_DB.PUBLIC.NUMERIC_TYPES where id = ${insertedId}`;
    stream<NumericType, sql:Error?> streamData = snowflakeClient->query(query);
    record {|NumericType value;|}? data = check streamData.next();
    check streamData.close();
    test:assertNotExactEquals(data?.value, (), "Incorrect InsetId returned.");
}

@test:Config {
    groups: ["execute", "execute-basic"],
    dependsOn: [testInsertAndSelectTableWithGeneratedKeys]
}
function testInsertWithAllNilAndSelectTableWithGeneratedKeys() returns error? {
    sql:ExecutionResult result = check snowflakeClient->execute(`insert into EXECUTE_DB.PUBLIC.NUMERIC_TYPES (int_type, bigint_type,
         smallint_type, tinyint_type, bit_type, decimal_type, numeric_type, float_type, real_type)
         values (null,null,null,null,null,null,null,null,null)`);

    test:assertExactEquals(result.affectedRowCount, 1, "Affected row count is different.");

    int insertedId = check snowflakeClient->queryRow(`select max(id) from EXECUTE_DB.PUBLIC.NUMERIC_TYPES AT(statement=>last_query_id())`);
    sql:ParameterizedQuery query = `SELECT * from EXECUTE_DB.PUBLIC.NUMERIC_TYPES where id = ${insertedId}`;
    stream<NumericType, sql:Error?> streamData = snowflakeClient->query(query);
    record {|NumericType value;|}? data = check streamData.next();
    check streamData.close();
    test:assertNotExactEquals(data?.value, (), "Incorrect InsetId returned.");
}

type StringData record {
    int id;
    string varchar_type;
    string charmax_type;
    string char_type;
    string charactermax_type;
    string character_type;
    string nvarcharmax_type;
    string longvarchar_type;
    string clob_type;
};

@test:Config {
    groups: ["execute", "execute-basic"],
    dependsOn: [testInsertWithAllNilAndSelectTableWithGeneratedKeys]
}
function testInsertWithStringAndSelectTable() returns error? {
    string intIDVal = "25";
    sql:ParameterizedQuery insertQuery = `Insert into EXECUTE_DB.PUBLIC.STRING_TYPES (id, varchar_type, charmax_type, char_type, charactermax_type,
        character_type, nvarcharmax_type, longvarchar_type, clob_type) values ( ${intIDVal}
        ,'str1','str2','s','str4','s','str6','str7','str8')`;
    sql:ExecutionResult result = check snowflakeClient->execute(insertQuery);

    test:assertExactEquals(result.affectedRowCount, 1, "Affected row count is different.");

    sql:ParameterizedQuery query = `SELECT * from EXECUTE_DB.PUBLIC.STRING_TYPES where id = ${intIDVal}`;
    stream<StringData, sql:Error?> streamData = snowflakeClient->query(query);
    record {|StringData value;|}? data = check streamData.next();
    check streamData.close();

    StringData expectedInsertRow = {
        id: 25,
        varchar_type: "str1",
        charmax_type: "str2",
        char_type: "s",
        charactermax_type: "str4",
        character_type: "s",
        nvarcharmax_type: "str6",
        longvarchar_type: "str7",
        clob_type: "str8"
    };
    test:assertEquals(data?.value, expectedInsertRow, "Incorrect InsetId returned.");
}

@test:Config {
    groups: ["execute", "execute-basic"],
    dependsOn: [testInsertWithStringAndSelectTable]
}
function testInsertWithEmptyStringAndSelectTable() returns error? {
    string intIDVal = "35";
    sql:ParameterizedQuery insertQuery = `Insert into EXECUTE_DB.PUBLIC.STRING_TYPES (id, varchar_type, charmax_type, char_type, charactermax_type,
         character_type, nvarcharmax_type, longvarchar_type, clob_type) values ( ${intIDVal},'','','','','','','','')`;
    sql:ExecutionResult result = check snowflakeClient->execute(insertQuery);
    test:assertExactEquals(result.affectedRowCount, 1, "Affected row count is different.");

    sql:ParameterizedQuery query = `SELECT * from EXECUTE_DB.PUBLIC.STRING_TYPES where id = ${intIDVal}`;
    stream<StringData, sql:Error?> streamData = snowflakeClient->query(query);
    record {|StringData value;|}? data = check streamData.next();
    check streamData.close();

    StringData expectedInsertRow = {
        id: 35,
        varchar_type: "",
        charmax_type: "",
        char_type: "",
        charactermax_type: "",
        character_type: "",
        nvarcharmax_type: "",
        longvarchar_type: "",
        clob_type: ""
    };
    test:assertEquals(data?.value, expectedInsertRow, "Incorrect InsetId returned.");
}

type StringNilData record {
    int id;
    string? varchar_type;
    string? charmax_type;
    string? char_type;
    string? charactermax_type;
    string? character_type;
    string? nvarcharmax_type;
    string? longvarchar_type;
    string? clob_type;
};

@test:Config {
    groups: ["execute", "execute-basic"],
    dependsOn: [testInsertWithEmptyStringAndSelectTable]
}
function testInsertWithNilStringAndSelectTable() returns error? {
    string intIDVal = "45";
    sql:ParameterizedQuery insertQuery = `Insert into EXECUTE_DB.PUBLIC.STRING_TYPES (id, varchar_type, charmax_type, char_type, charactermax_type,
         character_type, nvarcharmax_type, longvarchar_type, clob_type) values (${intIDVal},null,null,null,null,null,null,null,null)`;
    sql:ExecutionResult result = check snowflakeClient->execute(insertQuery);
    test:assertExactEquals(result.affectedRowCount, 1, "Affected row count is different.");

    sql:ParameterizedQuery query = `SELECT * from EXECUTE_DB.PUBLIC.STRING_TYPES where id = ${intIDVal}`;
    stream<StringNilData, sql:Error?> streamData = snowflakeClient->query(query);
    record {|StringNilData value;|}? data = check streamData.next();
    check streamData.close();

    StringNilData expectedInsertRow = {
        id: 45,
        varchar_type: (),
        charmax_type: (),
        char_type: (),
        charactermax_type: (),
        character_type: (),
        nvarcharmax_type: (),
        longvarchar_type: (),
        clob_type: ()
    };
    test:assertEquals(data?.value, expectedInsertRow, "Incorrect InsetId returned.");
}

@test:Config {
    groups: ["execute", "execute-basic"],
    dependsOn: [testInsertWithNilStringAndSelectTable]
}
function testInsertTableWithDatabaseError() returns error? {
    sql:ExecutionResult|sql:Error result = snowflakeClient->execute(`Insert into EXECUTE_DB.PUBLIC.NumericTypesNonExistTable (int_type) values (20)`);

    if result is sql:DatabaseError {
        test:assertTrue(result.message().includes("EXECUTE_DB.PUBLIC.NUMERICTYPESNONEXISTTABLE' does not exist or not authorized.."),
                        "Error message does not match, actual :'" + result.message() + "'");
        sql:DatabaseErrorDetail errorDetails = result.detail();
        test:assertEquals(errorDetails.errorCode, 2003, "SQL Error code does not match");
        test:assertEquals(errorDetails.sqlState, "42S02", "SQL Error state does not match");
    } else {
        test:assertFail("Database Error expected.");
    }
}

@test:Config {
    groups: ["execute", "execute-basic"],
    dependsOn: [testInsertTableWithDatabaseError]
}
function testInsertTableWithDataTypeError() returns error? {
    sql:ExecutionResult|sql:Error result = snowflakeClient->execute(`Insert into EXECUTE_DB.PUBLIC.NUMERIC_TYPES (int_type) values ('This is wrong type')`);

    if result is sql:DatabaseError {
        test:assertTrue(result.message().startsWith("Error while executing SQL query: Insert into EXECUTE_DB.PUBLIC.NUMERIC_TYPES" +
        " (int_type) values ('This is wrong type'). Numeric value 'This is wrong type' is not recognized."),
                    "Error message does not match, actual :'" + result.message() + "'");
        sql:DatabaseErrorDetail errorDetails = result.detail();
        test:assertEquals(errorDetails.errorCode, 100038, "SQL Error code does not match");
        test:assertEquals(errorDetails.sqlState, "22018", "SQL Error state does not match");
    } else {
        test:assertFail("Database Error expected.");
    }
}

type ResultCount record {
    int countVal;
};

@test:Config {
    groups: ["execute", "execute-basic"],
    dependsOn: [testInsertTableWithDataTypeError]
}
function testUpdateData() returns error? {
    sql:ExecutionResult result = check snowflakeClient->execute(`Update EXECUTE_DB.PUBLIC.NUMERIC_TYPES set int_type = 11 where int_type = 10`);
    test:assertExactEquals(result.affectedRowCount, 1, "Affected row count is different.");

    stream<ResultCount, sql:Error?> streamData = snowflakeClient->query(`SELECT count(*) as countval from EXECUTE_DB.PUBLIC.NUMERIC_TYPES
         where int_type = 11`);
    record {|ResultCount value;|}? data = check streamData.next();
    check streamData.close();
    test:assertEquals(data?.value?.countVal, 1, "Update command was not successful.");
}

@test:AfterSuite
function cleanUpExecuteDB() returns error? {
    _ = check snowflakeClient->execute(`DROP DATABASE IF EXISTS EXECUTE_DB`);
}
