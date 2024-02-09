Ballerina Snowflake Connector
===================

[![Build](https://github.com/ballerina-platform/module-ballerinax-snowflake/workflows/CI/badge.svg)](https://github.com/ballerina-platform/module-ballerinax-snowflake/actions?query=workflow%3ACI)
[![Trivy](https://github.com/ballerina-platform/module-ballerinax-snowflake/actions/workflows/trivy-scan.yml/badge.svg)](https://github.com/ballerina-platform/module-ballerinax-snowflake/actions/workflows/trivy-scan.yml)
[![codecov](https://codecov.io/gh/ballerina-platform/module-ballerinax-snowflake/branch/main/graph/badge.svg)](https://codecov.io/gh/ballerina-platform/module-ballerinax-snowflake)
[![GitHub Last Commit](https://img.shields.io/github/last-commit/ballerina-platform/module-ballerinax-snowflake.svg)](https://github.com/ballerina-platform/module-ballerinax-snowflake/commits/main)
[![GraalVM Check](https://github.com/ballerina-platform/module-ballerinax-snowflake/actions/workflows/build-with-bal-test-native.yml/badge.svg)](https://github.com/ballerina-platform/module-ballerinax-snowflake/actions/workflows/build-with-bal-test-native.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## Package overview

The [Snowflake](https://www.snowflake.com/) is a cloud-based data platform that provides a data warehouse as a service designed for the cloud, providing a single integrated platform with a single SQL-based data warehouse for all data workloads. 
The Snowflake data warehouse uses a new SQL database engine with a unique architecture designed for the cloud. It provides operations to execute a wide range of standard DDL Commands, SQL Commands, and SQL Functions for querying data sources.
You can find reference information for all the Snowflake SQL commands (DDL, DML, and query syntax) [here](https://docs.snowflake.com/en/sql-reference-commands.html).

The `ballerinax/snowflake` package allows you to access the Snowflake database via the Ballerina SQL APIs and manage data persistent in the Snowflake database.

## Set up guide

To use the Snowflake connector, you must have a valid Snowflake account. If you do not have an account, you can sign up for a account [here](https://signup.snowflake.com/).

### Create a warehouse and database

1. Log in to your Snowflake account.
2. Go to the **Warehouses** tab under the **Admin** section, as shown below.
   <img alt="Snowflake Warehouse" src="https://raw.githubusercontent.com/ballerina-platform/module-ballerinax-snowflake/main/docs/setup/resources/snowflakes_create_warehouse.png" width="75%" style='border:1px solid #000000'/>

3. Click **+ Warehouse** and select a name and type for a new warehouse, as shown below.
   <img alt="Snowflake Create Warehouse" src="https://raw.githubusercontent.com/ballerina-platform/module-ballerinax-snowflake/main/docs/setup/resources/snowflakes_create_warehouse_2.png" width="75%" style='border:1px solid #000000'/>

4. Optional - You can set the created warehouse as the default warehouse for the account by editing the profile settings, as shown below.
   <img alt="Snowflake Edit Profile" src="https://raw.githubusercontent.com/ballerina-platform/module-ballerinax-snowflake/main/docs/setup/resources/snokeflakes_user_profile.png" width="100%"/>
   <img alt="Snowflake set default warehouse" src="https://raw.githubusercontent.com/ballerina-platform/module-ballerinax-snowflake/main/docs/setup/resources/snowflakes_set_default_warehouse.png" width="75%" style='border:1px solid #000000'/>

*NOTE* If you do not set a default warehouse, you must specify the warehouse name when you create a connection to the Snowflake database.

5. Go to the **Databases** tab under the **Data** section and click **+ Database** to create a new database, as shown below.
   <img alt="Snowflake Database" src="https://raw.githubusercontent.com/ballerina-platform/module-ballerinax-snowflake/main/docs/setup/resources/snowflakes_create_database.png" width="75%" style='border:1px solid #000000'/>

*NOTE* Create a database can either be created using the Snowflake web interface or using the SQL command with the Snowflake connector.

## Quickstart

To use the snowflake connector in your Ballerina application, modify the .bal file as follows:

### Step 1: Import the connector

Import the `ballerinax/snowflake` package into your Ballerina project.
```ballerina
import ballerinax/snowflake;
```

### Step 2: Import the Snowflake driver into your Ballerina project

```ballerina
import ballerinax/snowflake.driver as _;
```

### Step 3: Instantiate a new connector

Create a Snowflake client endpoint by giving authentication details in the Snowflake configuration. 
```ballerina
Options options = {
    properties: {
        "JDBC_QUERY_RESULT_FORMAT": "JSON" // Optional. This 
    }
};

snowflake:Client snowflakeClient = check new(accountIdentifier, user, password, options);
```

### Step 4: Invoke the connector operation
Now, utilize the available connector operations.

#### Execute a DDL command
```ballerina
sql:ExecutionResult result = check snowflakeClient->execute(`CREATE TABLE COMPANY_DB.PUBLIC.EMPLOYEES (
        ID INT NOT NULL AUTOINCREMENT,
        FirstName VARCHAR(255),
        LastName VARCHAR(255),
        BusinessUnit VARCHAR(255),
        PRIMARY KEY (ID)
    )`);
```

#### Execute a DML command
```ballerina
sql:ExecutionResult result = check snowflakeClient->execute(`INSERT INTO COMPANY_DB.PUBLIC.EMPLOYEES (FirstName,
        LastName, BusinessUnit) VALUES ('Shawn', 'Jerome', 'Integration')`);
```

#### Execute a query
```ballerina
type Employee record {
    int id;
    string firstName;
    string lastName;
    string businessUnit;
};
...

stream<Employee, error?> resultStream = check snowflakeClient->query(`SELECT * FROM COMPANY_DB.PUBLIC.EMPLOYEES`);
```

#### Execute a query returning a single row
```ballerina
type Employee record {
    int id;
    string firstName;
    string lastName;
    string businessUnit;
};
...

Employee|error result = check snowflakeClient->queryRow(`SELECT * FROM COMPANY_DB.PUBLIC.EMPLOYEES WHERE ID = 1`);
```

#### Execute batch DML commands
```ballerina
sql:ExecutionResult[] result = check snowflakeClient->batchExecute([
    `INSERT INTO COMPANY_DB.PUBLIC.EMPLOYEES (FirstName, LastName, BusinessUnit) VALUES ('Shawn', 'Jerome', 'Integration')`,
    `INSERT INTO COMPANY_DB.PUBLIC.EMPLOYEES (FirstName, LastName, BusinessUnit) VALUES ('John', 'Doe', 'Integration')`
]);
```

#### Call a stored procedure
```ballerina
sql:ProcedureCallResult ret = check snowflakeClient->call(`{call PROCEDURES_DB.PUBLIC.SELECT_EMPLOYEE_DATA(1)}`, [Employee]);
stream<record {}, sql:Error?>? qResult = ret.queryResult;
```

### Examples

The following example shows how to use the Snowflake connector to create a table, insert data, and query data from the Snowflake database.

[Employees Data Management Example](https://github.com/ballerina-platform/module-ballerinax-snowflake/tree/master/examples/employees-db) - Manages employee data in a Snowflake database and exposes an HTTP service to interact with the database.

## Issues and projects

The **Issues** and **Projects** tabs are disabled for this repository as this is part of the Ballerina library. To report bugs, request new features, start new discussions, view project boards, etc., visit the Ballerina library [parent repository](https://github.com/ballerina-platform/ballerina-library).

This repository only contains the source code for the package.

## Building from the source

### Setting up the prerequisites

1. Download and install Java SE Development Kit (JDK) version 17. You can install either [OpenJDK](https://adoptopenjdk.net/) or [Oracle](https://www.oracle.com/java/technologies/downloads/).

    > **Note:** Set the JAVA_HOME environment variable to the path name of the directory into which you installed JDK.

2. Download and install [Ballerina Swan Lake](https://ballerina.io/). 

### Building the source

Execute the commands below to build from the source.

- To build the library:
    ```shell
    ./gradlew clean build
    ```
- To run the integration tests: 
    ```shell
    ./gradlew clean test
    ```

## Contributing to Ballerina

As an open source project, Ballerina welcomes contributions from the community. 

For more information, go to the [contribution guidelines](https://github.com/ballerina-platform/ballerina-lang/blob/master/CONTRIBUTING.md).

## Code of conduct

All the contributors are encouraged to read the [Ballerina Code of Conduct](https://ballerina.io/code-of-conduct).

## Useful links

* Discuss the code changes of the Ballerina project in [ballerina-dev@googlegroups.com](mailto:ballerina-dev@googlegroups.com).
* Chat live with us via our [Discord server](https://discord.gg/ballerinalang).
* Post all technical questions on Stack Overflow with the [#ballerina](https://stackoverflow.com/questions/tagged/ballerina) tag.
