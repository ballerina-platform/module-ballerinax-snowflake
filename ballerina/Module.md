## Overview
The [Ballerina](https://ballerina.io/) connector for [Snowflake](https://docs.snowflake.com/en/user-guide/jdbc.html) allows you to programmatically access all of the Snowflake applications, databases, APIs, services via the Java Database Connectivity (JDBC) API using [Ballerina](https://ballerina.io/).
It provides operations to execute a wide range of standard DDL Commands, SQL Commands, and SQL Functions for querying data sources. 
You can find reference information for all the Snowflake SQL commands (DDL, DML, and query syntax) [here](https://docs.snowflake.com/en/sql-reference-commands.html).

## Prerequisites

Before using this connector in your Ballerina application, complete the following:

### To connect to Snowflake

* Create a [Snowflake](https://signup.snowflake.com/) account.
* Obtain the `username` and `password` which you use to login to Snowflake account and `account_identifier` which uniquely identifies a Snowflake account within your business entity, as well as throughout the global network of Snowflake. 
 
## Quickstart

To use the Snowflake connector in your Ballerina application, update the .bal file as follows:

### Step 1: Import connector and driver
Import the following modules into the Ballerina project:
```ballerina
import ballerina/sql;
import ballerinax/snowflake;      // Get the Snowflake connector
import ballerinax/snowflake.driver as _;   // Get the Snowflake driver
```

### Step 2: Create a new connector instance
Provide the `account_identifier`, `<username>` and `<password>` to initialize the Snowflake connector. 
Options should be provided as follows, because `requestGeneratedKeys` option must be set to `snowflake:NONE` as snowflake does not support the retrieval of auto-generated keys.
```
snowflake:Options options = {
    requestGeneratedKeys: snowflake:NONE  // This should be specified
};
``` 
Depending on your requirement, you can also pass additional optional properties during the client connector initialization. 
For more information on connection string properties, see [Connection String Options](https://docs.snowflake.com/en/user-guide/jdbc.html).

* `<account_identifier>` is the unique identifies a Snowflake account.
* `<username>` is the username you use to login to Snowflake account.
* `<password>` is the password you use to login to Snowflake account.

```ballerina
string account_identifier = "<account_identifier>";  // Eg: "z******.europe-west4.gcp" 
string user = "<username>";
string password = "<password>";
snowflake:Options options = {
    requestGeneratedKeys: snowflake:NONE  // This should be specified
};

snowflake:Client snowflakeClient = check new (account_identifier, user, password, options);
```

You can also define `<account_identifier>`, `<username>` and `<password>` as configurable strings in your Ballerina program.

### Step 3: Invoke the connector operation
1. Use the Snowflake connector to consume all of the Snowflake applications, databases, APIs, services via the Java Database Connectivity (JDBC) API using Ballerina.

    Now let’s take a look at a few sample operations.

    Let’s assume,
    - `COMPANYDB` is the database name. 
    - `PUBLIC` is the schema name. 
    - `EMPLOYEES` is the table name

    Use the `query` operation to query data. 

    Following is a sample code to query data from a table.

    ```ballerina
    public function main() returns error? {
        sql:ParameterizedQuery sqlQuery = `SELECT * FROM COMPANYDB.PUBLIC.EMPLOYEES LIMIT 10`;
        stream<record {}, error?> resultStream = snowflakeClient->query(sqlQuery);

        check from record{} result in resultStream
            do {
                io:println("Full details of employee: ", result);
            };
    }
    ``` 

    Use the `execute` operation to perform DML and DDL operations.

    Following is a sample code to insert data into a table

    ```ballerina
    public function main() returns error? {
        sql:ParameterizedQuery sqlQuery = `INSERT INTO COMPANYDB.PUBLIC.EMPLOYEES (FirstName,
            LastName, Company) VALUES ('Shawn', 'Jerome', 'WSO2')`;
        _ = check snowflakeClient->execute(sqlQuery);
    }
    ```

    Use the `batchExecute` operation to perform a batch of DML and DDL operations.

    Following is a sample code to insert multiple records into a table

    ```ballerina
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
    ```
    Use the `call` operation to execute a stored procedure.

    Following is a sample code to execute the stored procedure named `getEmployeeInfo`

    ```ballerina
    public function main() error? {
        sql:ParameterizedCallQuery sqlQuery = `{CALL COMPANYDB.PUBLIC.getEmployeeInfo()}`;
        _ = check snowflakeClient->call(sqlQuery);
    }
    ```

2. Use `bal run` command to compile and run the Ballerina program.
