// Copyright (c) 2022 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
//
// WSO2 Inc. licenses this file to you under the Apache License,
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
import ballerinax/snowflake;      // Get the Snowflake connector
import ballerinax/snowflake.driver as _;   // Get the Snowflake driver

// Connection Configurations
configurable string accountIdentifier = ?;
configurable string user = ?;
configurable string password = ?;

snowflake:Options options = {
    requestGeneratedKeys: snowflake:NONE
};

// Initialize the Snowflake client
snowflake:Client snowflakeClient = check new (accountIdentifier, user, password, options);
public function main() returns error? {
    var insertRecords = [
        {
            FirstName: "Gloria",
            LastName: "Shania",
            Company: "ABC"
        }, 
        {
            FirstName: "Shane",
            LastName: "Warny",
            Company: "BCA"
        }, 
        {
            FirstName: "Neo",
            LastName: "Mark",
            Company: "CAB"
        }
    ];
    sql:ParameterizedQuery[] insertQueries = 
        from var data in insertRecords
        select `INSERT INTO COMPANYDB.PUBLIC.EMPLOYEES
                (FirstName, LastName, Company)
                VALUES (${data.FirstName}, ${data.LastName}, ${data.Company})`;
    _ = check snowflakeClient->batchExecute(insertQueries);
}
