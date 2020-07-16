module sqlbuilder.dataset;

import sqlbuilder.uda;
import sqlbuilder.types;
import sqlbuilder.traits;

TableDef buildTableDef(alias relation, mapping m)(TableDef dependency)
{
    auto tableid = makeSpec(dependency.as ~ "_" ~ relation.name, Spec.tableid);
    auto expr = ExprString(getTableName!(relation.foreign_table).makeSpec(Spec.id),
        " AS ", tableid[2 .. $].makeSpec(Spec.id), " ON (", tableid, m.foreign_key.makeSpec(Spec.id),
        " = ", dependency.as.makeSpec(Spec.tableid), m.key.makeSpec(Spec.id), ")");
    return TableDef(tableid[2 .. $], expr, [dependency]);
}

TableDef buildTableDef(T)(string rootName = null)
{
    auto expr = ExprString([getTableName!T.makeSpec(Spec.id)]);
    if(rootName == null)
        rootName = getTableName!T;
    if(rootName != getTableName!T)
    {
        expr ~= " AS ";
        expr ~= rootName.makeSpec(Spec.id);
    }
    return TableDef(rootName, expr);
}

template staticTableDef(alias relation, mapping m, TableDef dependency)
{
    static const TableDef staticTableDef = buildTableDef!(relation, m)(dependency);
}

template staticTableDef(T)
{
    static const TableDef staticTableDef = buildTableDef!(T)();
}

/*auto quoteIdentifier2(string name)
{
    import std.range : chain;
    return chain(`"`, name, `"`);
}*/

private ColumnDef!T makeColumnDef(T)(const TableDef table, string tablename, string colname)
{
    return ColumnDef!T(table, ExprString([tablename.makeSpec(Spec.tableid), colname.makeSpec(Spec.id)]));
}

template DataSet(T)
{
    alias DataSet = DataSet!(T, staticTableDef!(T));
}

struct DataSet(T, alias core)
{
    alias RowType = T;
    enum tableDef = core;
    enum anyNull = core.dependencies.length != 0;
    @property auto opDispatch(string item)() if (isField!(T, item))
    {
        // This is a column of the row, so just build the correct column definition.
        import std.typecons : Nullable;
        static if(anyNull)
            alias X = Nullable!(typeof(__traits(getMember, T, item)));
        else
            alias X = typeof(__traits(getMember, T, item));
        static col = makeColumnDef!(X) (core, core.as,
                        getColumnName!(__traits(getMember, T, item)));
        return col;
    }

    @property auto opDispatch(string item)() if (isRelation!(T, item))
    {
        // this is a relationship, use the UDAs assigned to the appropriate
        // item to generate the new dataset.
        enum field = getRelationField!(T, item);
        static assert(field != null);
        enum relation = getRelationFor!(__traits(getMember, T, field));
        enum m = getMappingFor!(__traits(getMember, T, field));
        return .DataSet!(relation.foreign_table, staticTableDef!(relation, m, core)).init;
    }

    // shortcut for all columns
    @property auto all()
    {
        import std.typecons : Nullable;
        static if(anyNull)
            alias X = Nullable!T;
        else
            alias X = T;
        static col = ColumnDef!X(core, ExprString(core.as.makeSpec(Spec.id), ".*",
                                                  makeSpec("", Spec.objend)));
        return col;
    }
}

version(unittest)
{
    struct MyBool
    {
        bool _val;
        string dbValue() { return _val ? "Y" : "N"; }
        alias _val this;
        static MyBool fromDbValue(string item) {
            return MyBool(item == "Y" || item == "y");
        }
    }

    @tableName("author")
    static struct Author
    {
        string firstName;
        string lastName;
        @primaryKey @autoIncrement int id = -1;
        @colType("VARCHAR(1)") MyBool ynAnswer;

        // relations
        static @mapping("auth_id") @oneToMany!book() Relation books;
    }

    enum BookType {
        Reference,
        Fiction
    }
    static struct book
    {
        @unique @colName("name") @colType("VARCHAR(100)") string title;
        @manyToOne!Author("author") @colName("auth_id") int author_id;
        BookType book_type;
        @primaryKey @autoIncrement int id = -1;
    }
}

unittest
{
    import sqlbuilder.dialect.mysql;
    import sqlbuilder.types;
    import std.typecons : Nullable;
    import std.stdio;
    // create a dataset based on author
    DataSet!(Author) ds;
    with(ds)
    {
        auto q = select().where(lastName, " = ", "Alexandrescu".param).select(all, books.title).orderBy(books.title);
        writeln(q);
        writeln(q.sql);
        writeln(q.params);
    }

    DataSet!(book) ds2;
    //ds2._core = new DataSetCore;
    //ds2._core.root = buildTableDef!book;

    with(ds2)
    {
        Nullable!string s;
        auto q = select(all, author.books.title.as("other_book_title")).where(author.lastName, " = ", s.param);
        //pragma(msg, q.RowTypes);
        writeln(q.sql);
        writeln(q.params);

        s = "Alexandrescu";
        q = select(all, author.books.title.as("other_book_title")).where(author.lastName, " = ", s.param);
        writeln(q.sql);
        writeln(q.params);
        writeln(createTableSql!Author);
        writeln(createTableSql!book);
    }

    // try some inserts
    {
        auto i = insert(Author("Andrei", "Alexandrescu"));
        writeln(i.sql);
        writeln(i.params);

        i = insert(ds.tableDef).set(ds.firstName, "Andrei");
        writeln(i.sql);
        writeln(i.params);
    }

    // updates
    {
        auto u = update(Author("Steven", "Schveighoffer", 1));
        writeln(u.sql);
        writeln(u.params);

        u = set(ds.firstName, "George".param).where(ds.id, " = ", 5.param);
        writeln(u.sql);
        writeln(u.params);
    }

    // deletes
    {
        auto d = remove(Author("Steven", "Schveighoffer", 1));
        writeln(d.sql);
        writeln(d.params);
        d = removeFrom(ds.tableDef).havingKey(Author("Steven", "Schveighoffer", 1));
        writeln(d.sql);
        writeln(d.params);
        d = removeFrom(ds.tableDef).havingKey(ds.books, 1);
        writeln(d.sql);
        writeln(d.params);
        d = removeFrom(ds.tableDef).havingKey!Author(1);
        writeln(d.sql);
        writeln(d.params);
    }

    // TODO: CTFE support
    //auto blah = genSQL();
    //enum ctsql = genSQL();
    /*pragma(msg, ctsql);
    writeln(ctsql);*/
}
