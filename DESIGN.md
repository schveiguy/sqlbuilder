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

## Compile time idea

Build using a specialized varible type which just stores the typename. Basically just a string. Then generate a dialect based on that. Use the dialect just as you would to build any query.
Use that result to generate a function that takes a connection, and a set of parameters that match the type names. Inside that function, you would use the pre-built arrays to form the query, and then the variables would be filled from the parameters.
cttype would be a function that takes a column and generates a token based on the column type that can then be later turned into a type at compile time.

e.g.:

```d
static import ct = sqlbuilder.dialect.ct;
import sqlbuilder.dialect.mysql;
import sqlbuilder.dataset;

void main()
{
   enum ds = DataSet!Author.init;
   enum myq = (){ with(ct) return select(ds.name, ds.books.title).where(ds.books.rating, " = ", ds.name.cttype); }();
   auto conn = new Connection(...);
   foreach(authorName, booktitle; conn.fetch(bind!myq(5))) {
      ...
   }
}

This isn't complete, doesn't loook that great yet...
