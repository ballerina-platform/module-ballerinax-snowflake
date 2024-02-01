# Employees Data Management Example

## Overview
This example demonstrates how to use the Ballerina `Snowflake` module to execute statements and query a Snowflake database.

Here, a sample database is used to demonstrate the functionalities of the module. This sample database models a 
company's employees management system. The database contains a single table `Employee` which contains information 
regarding an employee such as their employee ID, name, contact details, hire date, employee ID of their manager, and '
their job title.

This consists of two separate examples, and covers the following features:
* Connection
* Query (`SELECT`)
* Query row
* Execution (`INSERT`, `UPDATE`, `DELETE`)
* Batch Execution

### 1. Setup Example
This example shows how to establish a connection to a Snowflake database with the required configurations and connection
parameters, create a database & table, and finally populate the table.

### 2. Service Example
This example shows how an HTTP RESTful service can be created to insert and retrieve data from the Snowflake database.

## Prerequisites

### 1. Setting the configuration variables
In the `Config.toml` file, set the configuration variables to correspond to your Snowflake server.
* `accountIdentifier`: The account identifier of your Snowflake server
* `user`: The username of your Snowflake account
* `password`: The password of your Snowflake account

*NOTE:* Snowflake driver fails with Java 16 and above because starting with JDK 16, strong encapsulation was turned on by default and one of the driver dependencies have employed the use of sun.misc.Unsafe along with reflection. This is not allowed in Java 16 and above. Therefore, to run this example, you need to use Java 15 or below. For more information, see [here](https://community.snowflake.com/s/article/JDBC-Driver-Compatibility-Issue-With-JDK-16-and-Later). If you are using Java 16 or above, you can use the following workaround to run this example:

* Export the following environment variable:
  ```shell
  export JDK_JAVA_OPTIONS="--add-opens java.base/java.nio=ALL-UNNAMED"
  ```
* Set Snowflake property `JDBC_QUERY_RESULT_FORMAT` to `JSON` as follows:
  ```ballerina
  snowflake:Options options = {
      properties: {
          "JDBC_QUERY_RESULT_FORMAT": "JSON"
      }
  };
  ```

### 2. Establishing the connection
* The following can be used to connect to a MySQL server using Ballerina
  ```ballerina
  snowflake:Client snowflakeClient = check new (accountIdentifier, user, password, options);
  ```

After establishing the connection, queries may be executed using the `snowflakeClient` as usual.
```ballerina
_ = check snowflakeClient->execute(`
    INSERT INTO COMPANY_DB.PUBLIC.EMPLOYEE (employee_id, first_name, last_name, email, phone, hire_date, manager_id)
    VALUES (10, 'John', 'Smith', 'john@smith.com', '483 299 111', '2021-08-20', 1, "Software Engineer");
`);

stream<Employee, sql:Error?> streamData = snowflakeClient->query("SELECT * FROM COMPANY_DB.PUBLIC.EMPLOYEE");
check from Employee emp in streamData
  do {
      io:println(emp);
  };
```

## Running the Example

### 1. Setup
This example illustrates the following
* How to establish a connection to your MySQL server
* How to create a database and table
* Populating the table

This example can be run by executing the command `bal run setup`.

### 2. Service
This example creates an HTTP service with the endpoint `/employees` on port 8080 that can be used to interact with the
database

#### 2.1 Get all employee details - method:`GET`
* This would query the Employees table and fetch details of all the employees present in it.
* Example CURL request:
  ```shell
  curl 'localhost:8080/employees'
  ```

#### 2.2 Get details on a single employee - method:`GET`
* This would retrieve the details of a single employee with the given employee ID.
* Example CURL request:
  ```shell
  curl 'localhost:8080/employees/3'
  ```

#### 2.3 Add a new employee - method:`POST`
* This would add a new employee to the table.
* Example CURL request:
  ```shell
  curl -X POST 'localhost:8080/employees/' \
  --header 'Content-Type: application/json' \
  --data-raw '{
    "first_name": "test",
    "last_name": "test",
    "email": "test@test.com",
    "phone": "882 771 110",
    "hire_date": {
      "year": 2021,
      "month": 12,
      "day": 16
    },
    "manager_id": 1,
    "job_title": "Sales Intern"
  }'
  ```  

#### 2.4 Update an employee's information - method:`PUT`
* This would update the details of a provided employee on the table.
* Example CURL request:
  ```shell
  curl -X PUT 'localhost:8080/employees/2' \
  --header 'Content-Type: application/json' \
  --data-raw '{
  "first_name": "test",
  "last_name": "test",
  "email": "test@test.com",
  "phone": "882 771 110",
  "hire_date": {
  "year": 2021,
  "month": 12,
  "day": 16
  },
  "manager_id": 1,
  "job_title": "Sales Intern"
  }'
  ```

#### 2.5 Delete an employee - method:`DELETE`
* This would delete the details of the employee with the provided ID from the table.
* Example CURL request:
  ```shell
  curl -X DELETE 'localhost:8080/employees/6'
  ```

### 2.6 Count - method: `GET`
* This would retrieve the total number of employees that are present in the table.
* Example CURL request:
  ```shell
  curl 'localhost:8080/employees/count'
  ```
  
### 2.7 Get subordinates - method: `GET`
* This would retrieve the list of employees that another is responsible for managing.
* Example CURL request:
  ```shell
  curl 'localhost:8080/employees/subordinates/1'
  ```

This example can be run by executing the command `bal run service`.
