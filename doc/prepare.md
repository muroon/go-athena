# Prepare Statements

You can use [Prepared Statements on Athena](https://docs.aws.amazon.com/athena/latest/ug/querying-with-prepared-statements.html).

## How to use

```
  db, _ := sql.Open("athena", "db=default&output_location=s3://results")
  
  // 1. Prepare 
  stmt, _ := db.PrepareContext(ctx, fmt.Sprintf("SELECT url, code FROM cloudfront WHERE code = ?"))
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

## Input Parameter Types in Execution Prepared Statements

### Acceptable Types

|Athena Parameter Type|Input Pamameter Type (Golang)|
| --- | --- |
|BOOLEAN|bool|
|TINYINT|int,int,int16,int32,int64,uint,uint8,uint16,uint32,uint64|
|SMALLINT|int,int,int16,int32,int64,uint,uint8,uint16,uint32,uint64|
|INT|int,int,int16,int32,int64,uint,uint8,uint16,uint32,uint64|
|BIGINT|int,int,int16,int32,int64,uint,uint8,uint16,uint32,uint64|
|FLOAT|string|
|DOUBLE|string|
|DEMICAL|string|
|CHAR|string|
|VARCHAR|string|
|STRING|string|
|DATE|string|
|TIMESTAMP|string|

```
// int parameter
intParam := 1
stmt, _ := db.PrepareContext(ctx, fmt.Sprintf("SELECT * FROM test_table WHERE int_column = ?"))
rows, _ := stmt.QueryContext(ctx, intParam) 
// `SELECT * FROM test_table WHERE int_column = 1`
```

```
// string parameter
stringParam := "string value"
stmt, _ := db.PrepareContext(ctx, fmt.Sprintf("SELECT * FROM test_table WHERE string_column = ?"))
rows, _ := stmt.QueryContext(ctx, stringParam) 
// `SELECT * FROM test_table WHERE string_column = 'string value'`
```

#### Numeric String

If you want to specify a parameter for Float type column on Athena, please use String type parameter whose value is all Numeric.

```
// for float column
floatParam := "3.144"
stmt, _ := db.PrepareContext(ctx, fmt.Sprintf("SELECT * FROM test_table WHERE float_column = ?"))
rows, _ := stmt.QueryContext(ctx, floatParam) 
// `SELECT * FROM test_table WHERE float_column = 3.144`
```

If you want to specify a string value with all numbers for a String type column on Athena, use SetForceNumericString.

```
// for string column
floatParam := "3.144"
stmt, _ := db.PrepareContext(ctx, fmt.Sprintf("SELECT * FROM test_table WHERE string_column = ?"))
ctx = SetForceNumericString(ctx)
rows, _ := stmt.QueryContext(ctx, floatParam) 
// `SELECT * FROM test_table WHERE string_column = '3.144'`
```
