# SQL Builder and database serializer

sqlbuilder is a small library that is used to make sql easier to write. It also can serve as an abstraction layer for multiple database implementations. Currently, only MySQL is supported, but it has been designed to allow for PostgreSQL and SQLite support. Other systems may also fit within the system, but I have not focused on anything other than those three.

## Implemented Features

* Declarative syntax for tables and joins
* Joins are automatically handled by the building functions. Joins are only used when data from that join is needed.
* Wrap any type that represents a row as a `DataSet` and it can be used to query that data.
* UDAs to specify relationships and specialized handling of columns. Relationships are provided in `DataSet` to allow joins automatically to be added.
* Add columns to select, orderings, conditional clauses at any time (out of order construction). Individual clauses must be built in order.
* Parameters are used in-line to allow more intuitive statements.
* Automatic handling of parameters to avoid SQL injection attacks.
* Supports easy inserts and updates of whole records.
* Can also update individual records without having a model to use.
* Create and drop database tables and relationships with compile-time generated SQL.
* Deserialize automatically data from the database into the appropriate structs or types, generating a range of those types automatically.
* Duck-typing of expressions, parameters, and table references that allows building one's own `DataSet`-like abstractions.
* `ExprString` type which allows building of strings without utilizing too much extra allocation, and allows specialized tokens within the string to be recognized by the various dialects.

## Planned features
* Compile-time generation of SQL for queries without parameters.
* Integration with PostgreSQL
* Integration with SQLite
* Support alternate allocation schemes (currently, everthing is done via GC arrays). Possibly some small-array optimization for `ExprString`.
* Investigate the possibility of avoiding allocating a full string for SQL queries.
* Add ways to insert related rows (for example, a record of an Author might provide an "addBook" function automatically which has as parameters the non-key related items).
