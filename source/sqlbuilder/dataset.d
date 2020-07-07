module sqlbuilder.dataset;

import sqlbuilder.uda;
import sqlbuilder.types;
import sqlbuilder.traits;

TableDef buildTableDef(alias relation, mapping m)(TableDef dependency)
{
    auto tableid = makeSpec(dependency.as ~ "_" ~ relation.name, Spec.id);
    auto expr = ExprString(getTableName!(relation.foreign_table).makeSpec(Spec.id),
        " AS ", tableid, " ON (", tableid, ".", m.foreign_key.makeSpec(Spec.id),
        " = ", dependency.as.makeSpec(Spec.id), ".", m.key.makeSpec(Spec.id), ")");
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
    return ColumnDef!T(table, ExprString([tablename.makeSpec(Spec.id), ".", colname.makeSpec(Spec.id)]));
}

template DataSet(T)
{
    alias DataSet = DataSet!(T, staticTableDef!(T));
}

struct DataSet(T, alias core)
{
    @property auto opDispatch(string item)() if (isField!(T, item))
    {
        // This is a column of the row, so just build the correct column definition.
        import std.format;
        static col = makeColumnDef!(typeof(__traits(getMember, T, item)))
            (core, core.as, getColumnName!(__traits(getMember, T, item)));
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
    @property ColumnDef!T all()
    {
        static col = ColumnDef!T(core, ExprString(core.as.makeSpec(Spec.id), ".*"));
        return col;
    }
}

version(unittest)
{
    @tableName("author")
    static struct Author
    {
        @primaryKey @autoIncrement int id;
        string firstName;
        string lastName;
        static @mapping("auth_id") @oneToMany!book() Relation books;
    }

    static struct book
    {
        @primaryKey @autoIncrement int id;
        @unique @colName("name") string title;
        @manyToOne!Author("author") @colName("auth_id") int author_id;
    }
}

unittest
{
    import sqlbuilder.dialect.mysql;
    import std.typecons : Nullable;
    import std.variant : Variant;
    import std.stdio;
    // create a dataset based on author
    DataSet!(Author) ds;
    with(ds)
    {
        auto q = select!Variant().where(lastName, " = ", "Alexandrescu".param).select(all, books.title).orderBy(ds.books.title);
        writeln(q.sql);
        writeln(q.params);
    }

    DataSet!(book) ds2;
    //ds2._core = new DataSetCore;
    //ds2._core.root = buildTableDef!book;

    with(ds2)
    {
        Nullable!string s;
        auto q = select!Variant(all, author.books.title.as("other_book_title")).where(author.lastName, " = ", s.param);
        pragma(msg, q.RowTypes);
        writeln(q.sql);
        writeln(q.params);

        s = "Alexandrescu";
        q = select!Variant(all, author.books.title.as("other_book_title")).where(author.lastName, " = ", s.param);
        writeln(q.sql);
        writeln(q.params);
        writeln(createTableSql!Author);
        writeln(createTableSql!book);
    }

    // TODO: CTFE support
    //auto blah = genSQL();
    //enum ctsql = genSQL();
    /*pragma(msg, ctsql);
    writeln(ctsql);*/
}
