# Prepared Statements

You can use [Prepared Statements on Athena](https://docs.aws.amazon.com/athena/latest/ug/querying-with-prepared-statements.html).

## How to use

```
  db, _ := sql.Open("athena", "db=default&output_location=s3://results")
  
  // 1. Prepare 
  stmt, _ := db.PrepareContext(ctx, "SELECT url, code FROM cloudfront WHERE code = ?")
  defer stmt.Close() // 3. Close
 
  // 2. Execute
  rows, _ := stmt.QueryContext(ctx, targetCode)
  defer rows.Close()
  
  for rows.Next() {
    var url string
    var code int
    rows.Scan(&url, &code)
  }
```

### 1. Prepare
- Run [PREPARE](https://docs.aws.amazon.com/athena/latest/ug/querying-with-prepared-statements.html#querying-with-prepared-statements-sql-statements) on Athena to create a Statement for use with Athena
- Create a Stmt object and keep statement_name inside
  - The stmt object is valid until Close method is executed
- Result Mode
  - Available under all Result Modes
  - ResultMode can be specified in PrepareContext
    ```
    rows, _ := stmt.PrepareContext(SetDLMode(ctx), sql) // Prepares a statement in DL Mode
    ```

### 2. Execute
- Run a prepared statement using [EXECUTE](https://docs.aws.amazon.com/athena/latest/ug/querying-with-prepared-statements.html#querying-with-prepared-statements-sql-statements) on Athena
- You can specify parameters 
- Use QueryContext and ExecContext methods

### 3. Close
- Run [DEALLOCATE PREPARE](https://docs.aws.amazon.com/athena/latest/ug/querying-with-prepared-statements.html#querying-with-prepared-statements-sql-statements) and delete the prepared statement
- Use the Close method of the Stmt object

## Examples

#### int parameter

```
intParam := 1
stmt, _ := db.PrepareContext(ctx, "SELECT * FROM test_table WHERE int_column = ?")
rows, _ := stmt.QueryContext(ctx, intParam) 
```
execute `SELECT * FROM test_table WHERE int_column = 1`

#### string parameter

```
stringParam := "string value"
stmt, _ := db.PrepareContext(ctx, "SELECT * FROM test_table WHERE string_column = ?")
rows, _ := stmt.QueryContext(ctx, stringParam) 
```
execute `SELECT * FROM test_table WHERE string_column = 'string value'`

#### float parameter

Neither float32 nor float64 is supported because digit precision will easily cause large problems.
If you want to set a parameter for float type column on Athena, please use string type parameter whose characters are all numeric.

```
// for float column
floatParam := "3.144"
stmt, _ := db.PrepareContext(ctx, "SELECT * FROM test_table WHERE float_column = ?")
rows, _ := stmt.QueryContext(ctx, floatParam) 
```
execute SQL `SELECT * FROM test_table WHERE float_column = 3.144`

|golang (string)|in SQL|
| --- | --- |
|"123"|123|
|"1.23"|1.23|
|"1.23a"|'1.23a'|

#### for numeric string parameter 

By default, numeric string parameter isn't converted to string type in SQL, as shown in the float parameter example.
If you want to set a numeric value for a string type column on Athena, put true in SetForceNumericString function.

```
// for string column
floatParam := "3.144"
stmt, _ := db.PrepareContext(ctx, "SELECT * FROM test_table WHERE string_column = ?")
ctx = SetForceNumericString(ctx, true) // set true
rows, _ := stmt.QueryContext(ctx, floatParam) 
```
execute SQL `SELECT * FROM test_table WHERE string_column = '3.144'`

**under setting true in SetForceNumericString**

|golang (string)|in SQL|
| --- | --- |
|"123"|'123'|
|"1.23"|'1.23'|
