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

import ballerina/jballerina.java;
import ballerina/sql;

# Represents a Snowflake database client.
@display {label: "Snowflake", iconPath: "icon.png"}
public isolated client class Client {
    *sql:Client;

    # Initializes the Snowflake Client. The client must be kept open throughout the application lifetime.
    #
    # + account_identifier - The Snowflake account identifier
    # + user - The username of the Snowflake account
    # + password - The password of the Snowflake account
    # + options - The Snowflake client properties
    # + connectionPool - The `sql:ConnectionPool` to be used for the connection. If there is no
    #                    `connectionPool` provided, the global connection pool (shared by all clients) will be used
    # + return - An `sql:Error` if the client creation fails
    public isolated function init(string account_identifier, string user, string password,
        Options? options = (), sql:ConnectionPool? connectionPool = ()) returns sql:Error? {
        string url = string `jdbc:snowflake://${account_identifier}.snowflakecomputing.com/`;
        ClientConfiguration clientConf = {url, authConfig: { user, password }, options, connectionPool};
        return createClient(self, clientConf, sql:getGlobalConnectionPool());
    }

    # Executes the query, which may return multiple results.
    # When processing the stream, make sure to consume all fetched data or close the stream.
    #
    # + sqlQuery - The SQL query such as `` `SELECT * from Album WHERE name=${albumName}` ``
    # + rowType - The `typedesc` of the record to which the result needs to be returned
    # + return - Stream of records in the `rowType` type
    remote isolated function query(sql:ParameterizedQuery sqlQuery, typedesc<record {}> rowType = <>)
    returns stream<rowType, sql:Error?> = @java:Method {
        'class: "io.ballerina.lib.snowflake.nativeimpl.QueryProcessor",
        name: "nativeQuery"
    } external;

    # Executes the query, which is expected to return at most one row of the result.
    # If the query does not return any results, `sql:NoRowsError` is returned.
    #
    # + sqlQuery - The SQL query such as `` `SELECT * from Album WHERE name=${albumName}` ``
    # + returnType - The `typedesc` of the record to which the result needs to be returned.
    #                It can be a basic type if the query result contains only one column
    # + return - Result in the `returnType` type or an `sql:Error`
    remote isolated function queryRow(sql:ParameterizedQuery sqlQuery, typedesc<anydata> returnType = <>)
    returns returnType|sql:Error = @java:Method {
        'class: "io.ballerina.lib.snowflake.nativeimpl.QueryProcessor",
        name: "nativeQueryRow"
    } external;

    # Executes the SQL query. Only the metadata of the execution is returned (not the results from the query).
    #
    # + sqlQuery - The SQL query such as `` `DELETE FROM Album WHERE artist=${artistName}` ``
    # + return - Metadata of the query execution as an `sql:ExecutionResult` or an `sql:Error`
    remote isolated function execute(sql:ParameterizedQuery sqlQuery)
    returns sql:ExecutionResult|sql:Error = @java:Method {
        'class: "io.ballerina.lib.snowflake.nativeimpl.ExecuteProcessor",
        name: "nativeExecute"
    } external;

    # Executes the SQL query with multiple sets of parameters in a batch. 
    # Only the metadata of the execution is returned (not results from the query).
    # If one of the commands in the batch fails, this will return an `sql:BatchExecuteError`. However, the driver may
    # or may not continue to process the remaining commands in the batch after a failure.
    #
    # + sqlQueries - The SQL query with multiple sets of parameters
    # + return - Metadata of the query execution as an `sql:ExecutionResult[]` or an `sql:Error`
    remote isolated function batchExecute(sql:ParameterizedQuery[] sqlQueries) returns sql:ExecutionResult[]|sql:Error {
        if sqlQueries.length() == 0 {
            return error sql:ApplicationError(" Parameter 'sqlQueries' cannot be empty array");
        }
        return nativeBatchExecute(self, sqlQueries);
    }

    # Executes a SQL query, which calls a stored procedure. This may or may not return results.
    #
    # + sqlQuery - The SQL query such as `` `CALL sp_GetAlbums();` ``
    # + rowTypes - `typedesc` array of the records to which the results need to be returned
    # + return - Summary of the execution and results are returned in an `sql:ProcedureCallResult`, or an `sql:Error`
    remote isolated function call(sql:ParameterizedCallQuery sqlQuery, typedesc<record {}>[] rowTypes = [])
    returns sql:ProcedureCallResult|sql:Error = @java:Method {
        'class: "io.ballerina.lib.snowflake.nativeimpl.CallProcessor",
        name: "nativeCall"
    } external;

    # Closes the SQL client and shuts down the connection pool.
    #
    # + return - Possible error when closing the client
    public isolated function close() returns sql:Error? = @java:Method {
        'class: "io.ballerina.lib.snowflake.nativeimpl.ClientProcessor",
        name: "close"
    } external;
}

# An additional set of configurations related to a database connection.
public type Options record {|
    # The driver class name to be used to get the connection
    string? datasourceName = ();
    # The database properties, which should be applied when getting the connection
    map<anydata>? properties = ();
|};

# An additional set of configurations for the JDBC Client to be passed internally within the module.
type ClientConfiguration record {|
    # The JDBC URL to be used for the database connection.
    string? url;
    # The authentication configuration for the Snowflake client.
    AuthConfig authConfig;
    # The JDBC client properties
    Options? options;
    # The `sql:ConnectionPool` to be used for the connection. If there is no `connectionPool` provided, the global connection pool (shared by all clients) will be used
    sql:ConnectionPool? connectionPool;
|};

# Represents the authentication configuration for the Snowflake client.
type AuthConfig BasicAuth|KeyBasedAuth;

# Represents the basic authentication configuration for the Snowflake client.
type BasicAuth record {
    # The username of the Snowflake account
    string user;
    # The password of the Snowflake account
    string password;
};

# Represents the key-based authentication configuration for the Snowflake client.
type KeyBasedAuth record {
    # The username of the Snowflake account
    string user;
    # The path to the private key file. The private key file must be in the PKCS#8 format.
    # Use forward slashes as file path separators on all operating systems, including Windows. The JDBC driver replaces forward slashes with the appropriate path separator for the platform.
    string privateKeyPath;
    # The passphrase for the private key file. If the private key file is encrypted, provide the passphrase to decrypt the file.
    string privateKeyPassphrase?;
};

isolated function createClient(Client jdbcClient, ClientConfiguration clientConf,
    sql:ConnectionPool globalConnPool) returns sql:Error? = @java:Method {
    'class: "io.ballerina.lib.snowflake.nativeimpl.ClientProcessor"
} external;

isolated function nativeBatchExecute(Client sqlClient, string[]|sql:ParameterizedQuery[] sqlQueries)
returns sql:ExecutionResult[]|sql:Error = @java:Method {
    'class: "io.ballerina.lib.snowflake.nativeimpl.ExecuteProcessor"
} external;
