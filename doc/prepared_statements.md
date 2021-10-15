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

```
intParam := 1
stringParam := "string value"
stmt, _ := db.PrepareContext(ctx, "SELECT * FROM test_table WHERE int_column = ? AND string_column = ?")
rows, _ := stmt.QueryContext(ctx, intParam, stringParam)
```
execute `SELECT * FROM test_table WHERE int_column = 1 and string_column = 'string value'`
