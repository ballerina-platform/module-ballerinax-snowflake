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
snowflake:Client snowflakeClient = check new (accountIdentifier, user, password, options);

public function main() returns error? {
    check createDatabase();
    check createAndPopulateEmployeesTable();
}

function createDatabase() returns error? {
    _ = check snowflakeClient->execute(`DROP DATABASE IF EXISTS COMPANY_DB`);
    _ = check snowflakeClient->execute(`CREATE DATABASE COMPANY_DB`);
}

function createAndPopulateEmployeesTable() returns error? {
    _ = check snowflakeClient->execute(`DROP TABLE IF EXISTS COMPANY_DB.PUBLIC.EMPLOYEE`);

    _ = check snowflakeClient->execute(`
        CREATE TABLE COMPANY_DB.PUBLIC.EMPLOYEE (
            employee_id INTEGER AUTOINCREMENT PRIMARY KEY,
            first_name  VARCHAR(255) NOT NULL,
            last_name   VARCHAR(255) NOT NULL,
            email       VARCHAR(255) NOT NULL,
            phone       VARCHAR(50) NOT NULL ,
            hire_date   DATE NOT NULL,
            manager_id  INTEGER REFERENCES COMPANY_DB.PUBLIC.EMPLOYEE(employee_id),
            job_title   VARCHAR(255) NOT NULL
        )
    `);

    Employee[] employees = [
        {
            first_name: "Michael",
            last_name: "Scott",
            email: "michael.scott@example.com",
            phone: "737 299 2772",
            hire_date: {year: 1994, month: 2, day: 29},
            manager_id: (),
            job_title: "CEO"
        },
        {
            first_name: "Jane",
            last_name: "McIntyre",
            email: "jane.mcintyre@example.com",
            phone: "737 299 1111",
            hire_date: {year: 1996, month: 12, day: 15},
            manager_id: 1,
            job_title: "Vice President - Marketing"
        },
        {
            first_name: "Tom",
            last_name: "Scott",
            email: "tom.scott@example.com",
            phone: "439 882 099",
            hire_date: {year: 1998, month: 3, day: 23},
            manager_id: 1,
            job_title: "Vice President - Sales"
        },
        {
            first_name: "Elizabeth",
            last_name: "Queen",
            email: "elizabeth.queen@example.com",
            phone: "881 299 1123",
            hire_date: {year: 1978, month: 8, day: 19},
            manager_id: 2,
            job_title: "Marketing Executive"
        },
        {
            first_name: "Sam",
            last_name: "Smith",
            email: "sam.smith@example.com",
            phone: "752 479 2991",
            hire_date: {year: 2001, month: 5, day: 29},
            manager_id: 3,
            job_title: "Sales Intern"
        }
    ];

    sql:ParameterizedQuery[] insertQueries =
        from var emp in employees
        select `
            INSERT INTO COMPANY_DB.PUBLIC.EMPLOYEE
                (first_name, last_name, email, phone, hire_date, manager_id, job_title)
            VALUES
                (${emp.first_name}, ${emp.last_name}, ${emp.email}, ${emp.phone}, ${emp.hire_date},
                ${emp.manager_id}, ${emp.job_title})
            `;

    _ = check snowflakeClient->batchExecute(insertQueries);
}
