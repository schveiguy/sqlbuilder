module sqlbuilder.dataset;

import sqlbuilder.uda;
import sqlbuilder.types;
import sqlbuilder.traits;

TableDef buildTableDef(T, alias relation, mappings...)(Spec joinType, TableDef dependency)
{
    assert(isJoin(joinType), "Error, invalid spec for join with dependencies: " ~ joinType);
    auto tableid = makeSpec(dependency.as ~ "_" ~ joinType ~ "_" ~ relation.name, Spec.tableid);
    auto deptable = dependency.as.makeSpec(Spec.tableid);
    auto expr = ExprString(makeSpec(joinType), getTableName!(relation.foreign_table).makeSpec(Spec.id),
        " AS ", tableid[2 .. $].makeSpec(Spec.id), " ON (");

    static string genKeyId(T, string fieldname)()
    {
        return getColumnName!(__traits(getMember, T, fieldname)).makeSpec(Spec.id);
    }

    // static
    foreach(i, m; mappings)
    {
        static if(i != 0)
            expr ~= " AND ";
        static if(isKeyLiteral(m.foreign_key))
        {
            // the foreign key is a value, which means we need to put it into
            // the expression as-is. However, if the key is not a value, we
            // need to swap the order.
            static if(isKeyLiteral(m.key))
            {
                expr ~= ExprString(m.foreign_key, m.key);
            }
            else
            {
                expr ~= ExprString(deptable, genKeyId!(T, m.key), m.foreign_key);
            }
        }
        else static if(isKeyLiteral(m.key))
            expr ~= ExprString(tableid, genKeyId!(relation.foreign_table, m.foreign_key), m.key);
        else
            expr ~= ExprString(tableid, genKeyId!(relation.foreign_table, m.foreign_key), " = ", deptable, genKeyId!(T, m.key));
    }

    expr ~= ")";

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

template staticTableDef(T, alias relation, Spec joinType, TableDef dependency, m...)
{
    static const TableDef staticTableDef = buildTableDef!(T, relation, m)(joinType, dependency);
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
    static private bool _checkForLeftJoins(const TableDef td)
    {
        if(td.joinType == Spec.leftJoin) return true;
        foreach(dep; td.dependencies)
            if(_checkForLeftJoins(dep)) return true;
        return false;
    }

    enum anyNull = _checkForLeftJoins(core);

    @property auto opDispatch(string item)() if (isField!(T, item))
    {
        // This is a column of the row, so just build the correct column definition.
        import std.typecons : Nullable;
        static if(anyNull && !is(typeof(__traits(getMember, T, item)) : Nullable!U, U))
            alias X = Nullable!(typeof(__traits(getMember, T, item)));
        else
            alias X = typeof(__traits(getMember, T, item));
        static col = makeColumnDef!(X) (core, core.as,
                        getColumnName!(__traits(getMember, T, item)));
        return col;
    }

    template opDispatch(string item) if (isRelation!(T, item))
    {
        @property auto opDispatch(Spec joinType = Spec.none)() if (joinType == joinType.none || isJoin(joinType))
        {
            // this is a relationship, use the UDAs assigned to the appropriate
            // item to generate the new dataset.
            enum field = getRelationField!(T, item);
            static assert(field != null);
            enum relation = getRelationFor!(__traits(getMember, T, field));
            alias m = getMappingsFor!(__traits(getMember, T, field));
            enum realJoinType = joinType == Spec.none ? (relation.joinType == Spec.none ? Spec.leftJoin : relation.joinType) : joinType;
            return .DataSet!(relation.foreign_table, staticTableDef!(T, relation, realJoinType, core, m)).init;
        }
    }

    // shortcut for all columns
    @property auto allColumns()
    {
        import std.typecons : Nullable;
        static if(anyNull)
            alias X = Nullable!T;
        else
            alias X = T;
        static col = ColumnDef!X(core, ExprString(core.as.makeSpec(Spec.id), ".*",
                                                  objEndSpec));
        return col;
    }
}

// gets a data set that is defined by the related table. Used when it's too
// cumbersome to define all the various relations in the primary table.
//
// if relationName is null, and there is only one relation between the tables,
// then that relation is used. If relationName is not null, then the relation
// must be that name. A relation name is required when 2 relations between the
// types exist.
//
// The join name is kind of convoluted, but hard to make a unique one that
// reads well in English. perhaps this can be improved (maybe reverse the table
// definitions up to that point).
auto related(T, string relationName = null, Spec joinType = Spec.none, DS1)(DS1 dataset) if (isDataSet!DS1 && (joinType == Spec.none || isJoin(joinType)))
{
    static if(relationName.length)
    {
        static assert(isRelation!(T, relationName),
                      "No relation named " ~ relationName ~ " for " ~ T.stringof
                      ~ " to " ~ DS1.RowType.stringof);
        enum relatedField = getRelationField!(T, relationName);
    }
    else
    {
        enum relatedField = getRelationField!(T, dataset.RowType);
        static assert(relatedField.length,
                      "No relation for " ~ T.stringof ~ " to "
                      ~ DS1.RowType.stringof);
    }

    // now have the related field, fetch the relation and the mapping, and do
    // it in reverse.
    static assert(relatedField != null);
    import std.meta : staticMap;
    template recipMapping(mapping m)
    {
        enum recipMapping = mapping(m.key, m.foreign_key);
    }
    alias mappings = staticMap!(recipMapping, getMappingsFor!(__traits(getMember, T, relatedField)));
    enum revRelation = getRelationFor!(__traits(getMember, T, relatedField));
    enum relation = TableReference!T(getTableName!T ~ "_having_" ~ revRelation.name, revRelation.joinType);
    enum realJoin = joinType == Spec.none ? (relation.joinType == Spec.none ? Spec.leftJoin : relation.joinType) : joinType;
    return DataSet!(T, staticTableDef!(revRelation.foreign_table, relation, realJoin, dataset.tableDef, mappings)).init;
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
        static @mapping("author_id") @refersTo!book() Relation books;
        static @mapping("author_id") @mapping("book_type", " = 0") @refersTo!book() Relation referenceBooks;
    }

    enum BookType {
        Reference,
        Fiction
    }
    static struct book
    {
        @unique @colName("name") @colType("VARCHAR(100)") string title;
        @refersTo!Author("author", Spec.innerJoin) @colName("auth_id") int author_id;
        BookType book_type;
        @primaryKey @autoIncrement int id = -1;
    }
    static struct review
    {
        @refersTo!book("book") int book_id;
        @allowNull string comment;
        @allowNull(-1) int rating;
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
        auto q = select().where(lastName, " = ", "Alexandrescu".param).select(ds, books.title).orderBy(books.title);
        //writeln(q);
        writeln(q.sql);
        writeln(q.params);
        auto q2 = select(ds, referenceBooks.title).where(lastName, " = ", "Alexandrescu".param).orderBy(referenceBooks.title);
        writeln(q2.sql);
        writeln(q2.params);
    }

    DataSet!(book) ds2;

    with(ds2)
    {
        import std.range;
        static struct Optional(T)
        {
            Parameter!T value;
            bool valid = false;
            alias value this;
        }
        Optional!string s;
        auto q = select(ds2, author.related!book.title.as("other_book_title")).where(author.lastName, " = ", s);
        //pragma(msg, q.RowTypes);
        writeln(q.sql);
        writeln(q.RowTypes.stringof);
        writeln(q.params);

        s.params = only("Alexandrescu");
        s.valid = true;
        auto q2 = select(allColumns, ds2.author.books!(Spec.innerJoin).title.as("other_book_title")).where(ds2.author.lastName, " = ", s);
        writeln(q2.sql);
        writeln(q2.RowTypes.stringof);
        writeln(q2.params);
        writeln(createTableSql!Author);
        writeln(createTableSql!book);
    }

    // try some inserts
    {
        auto i = insert(Author("Andrei", "Alexandrescu"));
        writeln(i.sql);
        writeln(i.params);

        i = insert(ds.tableDef).set(ds.firstName, "Andrei".param);
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
        d = removeFrom(ds.tableDef).havingKey(ds.books!(Spec.innerJoin), 1);
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
