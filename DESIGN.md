# SQL Builder for DLang Design document

SQLBuilder intends to be a mechanism which uses structs and functions to build an SQL query programmatically instead of using only strings. It also has several mechanisms intended to make generating queries dynamically easier, especially when used with optional parameters.

## Design goals

1. Manage table dependencies for SQL. This allows one to only include joins when they are relevant.
2. Allow straight SQL usage when possible. I should not have to use special mechanisms for e.g. "a < b", I should be able to write "a < b" and the library should understand what's happening if necessary.
3. Minimal usage of structs and functions. SQL is at heart a text-based language, and while some code is needed to optionally include items or not, most things are best stored simply as strings. I do not want to type everything out as function/struct calls.
4. Allow multiple backends. SQL has various dialects for various database backends. SQLBuilder should support as many as possible by abstracting the commonality.
5. Do not string-ify values. Use actual values where data is used, and forward this to the back-end's system of prepared statements.
6. Support complex clauses such as AND and OR, even with data supplied.
7. Support out of order SQL building. In other words, I can specify where clauses before I specify columns I desire.
8. Optional - support nested SQL.
9. Abstract some specifics of dialects of SQL. For example, row fetch limits, or offsets.
10. Resulting SQL is accessible via a non-allocating range. Backend libraries can choose to support this if desired. Of course, toString will also be supported.
11. Allow storing clauses separate from the SQLQuery struct for piecing together items. For instance, I should be able to separate how I build a where clause into it's various pieces, and then put them all together at the end.
12. Allow definition of table names and common aliases for easy usage later.
13. Support optional inclusion of clauses based on an Optional wrapper.
14. Minimal allocations. Some allocation will obviously be necessary, but not required.
15. Allow building of SQL at compile time. Might need a separate set of functions for this, when variables are neccessary.
