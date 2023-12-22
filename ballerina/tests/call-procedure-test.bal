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
function setupProcedureDB() returns error? {
    _ = check snowflakeClient->execute(`CREATE DATABASE IF NOT EXISTS PROCEDURES_DB`);

    _ = check snowflakeClient->execute(`CREATE TEMPORARY TABLE IF NOT EXISTS PROCEDURES_DB.PUBLIC.STRING_TYPES (
                                                                                                         ID INTEGER,
                                                                                                         VARCHAR_TYPE VARCHAR(255),
                                                                                                         CHARMAX_TYPE CHAR(10),
                                                                                                         CHAR_TYPE CHAR,
                                                                                                         CHARACTERMAX_TYPE CHARACTER(10),
                                                                                                         CHARACTER_TYPE CHARACTER,
                                                                                                         NVARCHARMAX_TYPE NVARCHAR(255),
                                                                                                         PRIMARY KEY (ID)
                                                                                                        )`);
    _ = check snowflakeClient->execute(`INSERT INTO PROCEDURES_DB.PUBLIC.STRING_TYPES (ID, VARCHAR_TYPE, CHARMAX_TYPE, CHAR_TYPE, CHARACTERMAX_TYPE, CHARACTER_TYPE, NVARCHARMAX_TYPE)
                                                                                                                VALUES (1, 'test0', 'test1', 'a', 'test2', 'b', 'test3')`);

    _ = check snowflakeClient->execute(`CREATE OR REPLACE PROCEDURE PROCEDURES_DB.PUBLIC.INSERT_STRING_DATA (ID INTEGER,
                                                                          VARCHAR_TYPE VARCHAR(255),
                                                                          CHARMAX_TYPE CHAR(10),
                                                                          CHAR_TYPE CHAR,
                                                                          CHARACTERMAX_TYPE CHARACTER(10),
                                                                          CHARACTER_TYPE CHARACTER,
                                                                          NVARCHARMAX_TYPE NVARCHAR(255))
                                        RETURNS VARCHAR(255)
                                        LANGUAGE SQL
                                        AS
                                        $$
                                        BEGIN
                                            INSERT INTO PROCEDURES_DB.PUBLIC.STRING_TYPES (ID, VARCHAR_TYPE, CHARMAX_TYPE, CHAR_TYPE, CHARACTERMAX_TYPE, CHARACTER_TYPE, NVARCHARMAX_TYPE)
                                            VALUES (:ID, :VARCHAR_TYPE, :CHARMAX_TYPE, :CHAR_TYPE, :CHARACTERMAX_TYPE, :CHARACTER_TYPE, :NVARCHARMAX_TYPE);
                                            RETURN 'SUCCESS';
                                        END;
                                        $$;`);

    _ = check snowflakeClient->execute(`CREATE OR REPLACE PROCEDURE PROCEDURES_DB.PUBLIC.SELECT_STRING_DATA (p_id INTEGER)
                                        RETURNS TABLE (varchar_type VARCHAR(255),
                                                        charmax_type CHAR(10),
                                                        char_type CHAR,
                                                        charactermax_type CHARACTER(10),
                                                        character_type CHARACTER,
                                                        nvarcharmax_type NVARCHAR(255))
                                        LANGUAGE SQL
                                        AS
                                        $$
                                        DECLARE
                                          select_statement VARCHAR;
                                          res RESULTSET;
                                        BEGIN
                                            select_statement := 'SELECT varchar_type, charmax_type, char_type, charactermax_type, character_type,
                                                                                    nvarcharmax_type FROM PROCEDURES_DB.PUBLIC.STRING_TYPES WHERE ID = ' || :p_id;
                                            res := (EXECUTE IMMEDIATE :select_statement);
                                            RETURN TABLE(res);
                                        END;
                                        $$;`);
}

type StringDataForCall record {
    string varchar_type;
    string charmax_type;
    string char_type;
    string charactermax_type;
    string character_type;
    string nvarcharmax_type;
};

type StringDataSingle record {
    string varchar_type;
};

@test:Config {
    groups: ["procedures"],
    enable: true
}
function testCallWithStringTypes() returns error? {
    sql:ProcedureCallResult procResult = check snowflakeClient->call(`{CALL PROCEDURES_DB.PUBLIC.INSERT_STRING_DATA (ID => 2, VARCHAR_TYPE => 'test1', CHARMAX_TYPE => 'test2', CHAR_TYPE => 'c', CHARACTERMAX_TYPE => 'test3', CHARACTER_TYPE => 'd', NVARCHARMAX_TYPE => 'test4')}`);

    stream<record {}, sql:Error?>? qResult = procResult.queryResult;
    if qResult is () {
        test:assertFail("Empty result set returned.");
    } else {
        record {|record {} value;|}? data = check qResult.next();
        if data is () {
            test:assertFail("Empty result set returned.");
        } else {
            record {} value = data.value;
            test:assertEquals(value["INSERT_STRING_DATA"], "SUCCESS", "Call procedure insert and query did not match.");
        }
        check qResult.close();
    }
    check procResult.close();

    sql:ParameterizedQuery sqlQuery = `SELECT varchar_type, charmax_type, char_type, charactermax_type, character_type,
                   nvarcharmax_type from PROCEDURES_DB.PUBLIC.STRING_TYPES where ID = 2`;

    StringDataForCall expectedDataRow = {
        varchar_type: "test1",
        charmax_type: "test2",
        char_type: "c",
        charactermax_type: "test3",
        character_type: "d",
        nvarcharmax_type: "test4"
    };

    StringDataForCall storedData = check snowflakeClient->queryRow(sqlQuery);
    test:assertEquals(storedData, expectedDataRow, "Call procedure insert and query did not match.");
}

@test:Config {
    groups: ["procedures"]
}
function testCallWithStringTypesReturnsData() returns error? {
    sql:ProcedureCallResult ret = check snowflakeClient->call(`{call PROCEDURES_DB.PUBLIC.SELECT_STRING_DATA(1)}`, [StringDataForCall]);
    stream<record {}, sql:Error?>? qResult = ret.queryResult;
    if qResult is () {
        test:assertFail("Empty result set returned.");
    } else {
        record {|record {} value;|}? data = check qResult.next();
        record {}? value = data?.value;
        StringDataForCall expectedDataRow = {
            varchar_type: "test0",
            charmax_type: "test1",
            char_type: "a",
            charactermax_type: "test2",
            character_type: "b",
            nvarcharmax_type: "test3"
        };
        test:assertEquals(value, expectedDataRow, "Call procedure insert and query did not match.");
        check qResult.close();
        check ret.close();
    }
}

@test:AfterSuite
function cleanupProcedureDB() returns error? {
    _ = check snowflakeClient->execute(`DROP DATABASE IF EXISTS PROCEDURES_DB`);
}
