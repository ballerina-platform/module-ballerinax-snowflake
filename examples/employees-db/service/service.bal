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

import ballerina/time;
import ballerina/http;
import ballerina/sql;
import ballerinax/snowflake;
import ballerinax/snowflake.driver as _;

// Connection Configurations
configurable string accountIdentifier = ?;
configurable string user = ?;
configurable string password = ?;

snowflake:Options options = {
    properties: {
        "JDBC_QUERY_RESULT_FORMAT": "JSON"
    }
};

public type Employee record {
    int employee_id?;
    string first_name;
    string last_name;
    string email;
    string phone;
    time:Date hire_date;
    int? manager_id;
    string job_title;
};

// Initialize the Snowflake client
final snowflake:Client snowflakeClient = check new (accountIdentifier, user, password, options);

isolated service /employees on new http:Listener(8080) {

    isolated resource function get .() returns Employee[]|error? {
        stream<Employee, error?> resultStream = snowflakeClient->query(`SELECT * FROM COMPANY_DB.PUBLIC.EMPLOYEE`);
        return check from Employee employee in resultStream
                           select employee;
    }

    isolated resource function get [int id]() returns Employee|error? {
        return check snowflakeClient->queryRow(`SELECT * FROM COMPANY_DB.PUBLIC.EMPLOYEE WHERE employee_id = ${id}`);
    }

    isolated resource function post .(@http:Payload Employee emp) returns string|int|error? {
         _ = check snowflakeClient->execute(`
            INSERT INTO COMPANY_DB.PUBLIC.EMPLOYEE (first_name, last_name, email, phone, hire_date, manager_id, job_title)
            VALUES (${emp.first_name}, ${emp.last_name}, ${emp.email}, ${emp.phone}, ${emp.hire_date},
                    ${emp.manager_id}, ${emp.job_title})
        `);

        return check snowflakeClient->queryRow(`select max(employee_id) from COMPANY_DB.PUBLIC.EMPLOYEE AT(statement=>last_query_id())`);
    }

    isolated resource function put [int id](@http:Payload Employee emp) returns int|error? {
        sql:ExecutionResult result = check snowflakeClient->execute(`
            UPDATE COMPANY_DB.PUBLIC.EMPLOYEE
            SET first_name = ${emp.first_name}, last_name = ${emp.last_name}, email = ${emp.email},
                phone = ${emp.phone}, hire_date = ${emp.hire_date}, manager_id = ${emp.manager_id},
                job_title = ${emp.job_title}
            WHERE employee_id = ${id}
        `);
        return result.affectedRowCount;
    }

    isolated resource function delete [int id]() returns int|error? {
        sql:ExecutionResult result = check snowflakeClient->execute(`DELETE FROM COMPANY_DB.PUBLIC.EMPLOYEE WHERE employee_id = ${id}`);
        return result.affectedRowCount;
    }

    isolated resource function get count() returns int|error? {
        return check snowflakeClient->queryRow(`SELECT COUNT(*) FROM COMPANY_DB.PUBLIC.EMPLOYEE`);
    }

    isolated resource function get subordinates/[int id]() returns Employee[]|error? {
        stream<Employee, error?> resultStream = snowflakeClient->query(`SELECT * FROM COMPANY_DB.PUBLIC.EMPLOYEE WHERE manager_id = ${id}`);
        return check from Employee employee in resultStream
                           select employee;
    }
}
