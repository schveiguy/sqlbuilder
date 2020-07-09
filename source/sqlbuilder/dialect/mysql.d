module sqlbuilder.dialect.mysql;
public import sqlbuilder.dialect.common;
import sqlbuilder.types;
import sqlbuilder.traits;

private void sqlPut(bool includeObjectSeparators, bool includeTableQualifiers, App)(ref App app, ExprString expr)
{
    foreach(x; expr.data)
        sqlPut!(includeObjectSeparators, includeTableQualifiers)(app, x);
}

private void sqlPut(bool includeObjectSeparators, bool includeTableQualifiers, App)(ref App app, string x)
{
    import std.range : put;
    with(Spec) switch(getSpec(x))
    {
    case id:
        put(app, '`');
        put(app, x[2 .. $]);
        put(app, '`');
        break;
    case tableid:
        static if(includeTableQualifiers)
        {
            put(app, '`');
            put(app, x[2 .. $]);
            put(app, "`.");
        }
        break;
    case join: // TODO: implement other joins
        put(app, " LEFT JOIN ");
        break;
    case param:
        put(app, '?');
        break;
    case none:
        put(app, x);
        break;
    case objend:
        static if(includeObjectSeparators)
            put(app, ", 1 AS `_objend`");
        break;
    default:
        throw new Exception("Unknown spec in: " ~ x);
    }
}

auto params(T)(T t)
{
    static if(isQuery!T)
    {
        return paramsImpl!("fields", "joins", "conditions", "orders")(t);
    }
    else static if(is(T : Insert!P, P))
    {
        return paramsImpl!("colNames", "colValues")(t);
    }
    else static if(is(T : Update!P, P))
    {
        return paramsImpl!("joins", "settings", "conditions")(t);
    }
}

string sql(bool includeObjectSeparators = false, QP...)(Query!(QP) q)
{
    import std.array : Appender;
    import std.range : put;
    Appender!string app;
    assert(q.fields.expr);
    assert(q.joins.expr);

    // add a set of fragments given the prefix and the separator
    void addFragment(SQLFragment!(q.ItemType) item, string prefix)
    {
        if(item.expr)
        {
            put(app, prefix);
            sqlPut!(includeObjectSeparators, true)(app, item.expr);
        }
    }

    // fields
    addFragment(q.fields, "SELECT ");
    // joins
    addFragment(q.joins, " FROM ");
    // CONDITIONS
    addFragment(q.conditions, " WHERE ");
    // ORDER BY
    addFragment(q.orders, " ORDER BY ");

    return app.data;
}

string sql(Item)(Insert!Item ins)
{
    import std.array : Appender;
    import std.range : put;
    Appender!string app;
    assert(ins.tableid.length);
    assert(ins.colNames.expr);
    assert(ins.colValues.expr);

    put(app, "INSERT INTO `");
    put(app, ins.tableid);
    put(app, "` (");
    sqlPut!(false, false)(app, ins.colNames.expr);
    put(app, ") VALUES (");
    sqlPut!(false, false)(app, ins.colValues.expr);
    put(app, ")");

    return app.data;
}

string sql(Item)(Update!Item upd)
{
    // MySQL is quite forgiving, so just include everything as it is written.
    import std.array : Appender;
    import std.range : put;
    Appender!string app;
    assert(upd.settings.expr);
    assert(upd.joins.expr);

    // add a set of fragments given the prefix and the separator
    void addFragment(SQLFragment!(upd.ItemType) item, string prefix)
    {
        if(item.expr)
        {
            put(app, prefix);
            sqlPut!(false, true)(app, item.expr);
        }
    }

    // fields
    addFragment(upd.joins, "UPDATE ");
    // joins
    addFragment(upd.settings, " SET ");
    // CONDITIONS
    addFragment(upd.conditions, " WHERE ");

    return app.data;
}

private import std.meta : AliasSeq;
private import std.datetime : Date, DateTime, TimeOfDay;
private alias _typeMappings = AliasSeq!(
    byte, "TINYINT",
    short, "SMALLINT", 
    int, "INT",
    long, "BIGINT",
    ubyte, "TINYINT UNSIGNED", 
    ushort, "SMALLINT UNSIGNED",
    uint, "INT UNSIGNED",
    ulong, "BIGINT UNSIGNED",
    string, "TEXT",
    char[], "TEXT",
    const(char)[], "TEXT",
    ubyte[], "BLOB",
    const(ubyte)[], "BLOB",
    immutable(ubyte)[], "BLOB",
    float, "FLOAT",
    double, "DOUBLE",
    Date, "DATE",
    DateTime, "DATETIME",
    TimeOfDay, "TIME",
);

// match the D type to a specific MySQL type
template getFieldType(T)
{
    static foreach(i; 0 .. _typeMappings.length / 2)
        static if(is(T == _typeMappings[i * 2]))
            enum getFieldType = _typeMappings[i * 2 + 1];
}

// generate an SQL statement to insert a table definition.
template createTableSql(T)
{
    string generate()
    {
        import std.traits;
        import std.typecons : Nullable;
        import sqlbuilder.uda;
        import sqlbuilder.traits;
        auto result = "CREATE TABLE `" ~ getTableName!T ~ "` (";
        foreach(field; FieldNameTuple!T)
        {
            // if it's a relationship, we will save this for later
            alias fieldType = typeof(__traits(getMember, T, field));
            static if(!is(fieldType == Relation))
            {
                string name =
                result ~= "`" ~ getColumnName!(__traits(getMember, T, field)) ~ "` ";
                enum isNullable = isInstanceOf!(Nullable, fieldType);
                static if(isNullable)
                    result ~= getFieldType!(typeof(fieldType.init.get()));
                else
                    result ~= getFieldType!fieldType;
                static if(!isNullable)
                    result ~= " NOT NULL";
                static if(hasUDA!(__traits(getMember, T, field), unique))
                    result ~= " UNIQUE";
                static if(hasUDA!(__traits(getMember, T, field), autoIncrement))
                    result ~= " AUTO_INCREMENT";

                result ~= ",";
            }
        }
        // add primary key
        alias keyFields = getSymbolsByUDA!(T, primaryKey);
        static if(keyFields.length > 0)
        {
            result ~= " PRIMARY KEY (";
            static foreach(i, alias kf; keyFields)
            {
                static if(i != 0)
                    result ~= ",";
                result ~= "`" ~ getColumnName!(kf) ~ "`";
            }
            result ~= "),";
        }

        return result[0 .. $-1] ~ ")";
    }
    enum createTableSql = generate();
}

enum dropTableSql(T) = "DROP TABLE IF EXISTS `" ~ getTableName!T ~ "`";

// create relations between this database and it's related ones. All tables are
// expected to exist already.
template createRelations(T)
{
}


import std.typecons : Nullable;
import std.traits : isInstanceOf;
// check if a type is a primitive, vs. an aggregate with individual columns
private template isMysqlPrimitive(T)
{
    static if(isInstanceOf!(Nullable, T))
        alias isMysqlPrimitive = .isMysqlPrimitive!(typeof(T.init.get()));
    else
        enum isMysqlPrimitive = !is(T == struct) || is(T == Date)
               || is(T == DateTime) || is(T == TimeOfDay);
}

// if we have mysql native as a dependency, provide direct serialization from a ResultRange
version(Have_mysql_native)
{
    private template columnFieldNames(T)
    {
        static if(isMysqlPrimitive!T)
            // signal that no name is used.
            alias columnFieldNames = AliasSeq!("");
        else
        {
            static if(isInstanceOf!(Nullable, T))
                alias columnFieldNames = columnFieldNames!(typeof(T.init.get()));
            else
            {
                import std.meta : Filter;
                enum includeIt(string fieldname) = isField!(T, fieldname);
                alias columnFieldNames = Filter!(includeIt, __traits(allMembers, T));
            }
        }
    }

    import std.variant : Variant;
    private auto getLeaf(T)(Variant v) if (isMysqlPrimitive!T)
    {
        static if(isInstanceOf!(Nullable, T))
        {
            if(v.type == typeid(typeof(null)))
            {
                return T.init;
            }
            return T(v.get!(typeof(T.init.get())));
        }
        else
        {
            // null not tolerated
            return v.get!T;
        }
    }

    import mysql.result : Row;
    private auto getItem(T)(Row r, size_t[] colIds)
    {
        static if(isMysqlPrimitive!T)
        {
            return getLeaf!T(r[colIds[0]]);
        }
        else static if(isInstanceOf!(Nullable, T))
        {
            // TODO: we should not be using exception handling for this...
            try
            {
                T result = T(getItem!(typeof(T.init.get()))(r, colIds));
                return result;
            }
            catch(Exception e)
            {
                // just return nullable
                return T.init;
            }
        }
        else
        {
            
            T result;
            foreach(idx, n; columnFieldNames!T)
            {
                if(colIds[idx] != size_t.max)
                {
                    __traits(getMember, result, n) = getLeaf!(typeof(__traits(getMember, result, n)))(r[colIds[idx]]);
                }
            }
            return result;
        }
    }

    import mysql.connection;

    // fetch a range of serialized items
    auto fetch(bool throwOnExtraColumns = false, QD...)(Connection conn, Query!(QD) q)
    {
        import mysql.commands;
        import mysql.result;
        ResultRange seq;
        import std.stdio;
        writeln(q.sql!true);
        import std.range;
        if(q.params.empty)
        {
            // no parameters, just execute the query
            seq = conn.query(q.sql!true);
        }
        else
        {
            auto p = conn.prepare(q.sql!true);
            import std.range : enumerate;
            foreach(idx, arg; q.params.enumerate)
                p.setArg(idx, arg);
            seq = conn.query(p);
        }
        static if(q.RowTypes.length == 0 ||  is(q.RowTypes[0] == void))
        {
            // just return the range generated from the query, types weren't provided.
            return seq;
        }
        else
        {
            // serialization can happen, we have a type list.
            alias colid(T) = size_t[columnFieldNames!T.length];
            import std.meta : staticMap;
            alias colT = staticMap!(colid, q.RowTypes);
            static struct SerializedRange
            {
                private ResultRange seq;
                // TODO: make wrapper types for aggregates
                private q.RowTypes row; // here is where we store the front element
                private colT colIds;

                auto front()
                {
                    static if(row.length == 1)
                        return row[0];
                    else
                    {
                        import std.typecons : tuple;
                        return tuple(row);
                    }
                }

                bool empty() { return seq.empty; }

                private void loadItem()
                {
                    if(!seq.empty)
                    {
                        static foreach(i; 0 .. row.length)
                        {
                            row[i] = getItem!(q.RowTypes[i])(seq.front,
                                                              colIds[i][]);
                        }
                    }
                }

                void popFront()
                {
                    seq.popFront;
                    loadItem();
                }
            }

            SerializedRange result;
            result.seq = seq;
            if(seq.empty)
                return result;

            // map all column names to indexes
            size_t colIdx = 0;
            auto colNames = seq.colNames;
            // have to wrap this in a function, because otherwise, I
            // get multiple defined labels.

            foreach(idx, T; q.RowTypes)
            {
                static if(isMysqlPrimitive!T)
                {
                    assert(colIdx < colNames.length);
                    result.colIds[idx][0] = colIdx;
                    ++colIdx;
                }
                else
                () { // lambda because of the labels
                    static assert(!isMysqlPrimitive!T);
                    static if(isInstanceOf!(Nullable, T))
                        alias realT = typeof(T.init.get());
                    else
                        alias realT = T;
                    bool objEndFound = false;
                    result.colIds[idx][] = size_t.max;
objLoop:
                    while(colIdx < colNames.length)
                    {
objSwitch:
                        switch(colNames[colIdx])
                        {
                        case "_objend":
                            ++colIdx;
                            break objLoop;

                            static foreach(fnum, fname; columnFieldNames!realT)
                            {
                            case getColumnName!(__traits(getMember, realT, fname)):
                                result.colIds[idx][fnum] = colIdx;
                                break objSwitch;
                            }

                        default:
                            static if(throwOnExtraColumns)
                                throw new Exception("Unknown column name found: " ~ colNames[colIdx]);
                            else
                                break;
                        }
                        ++colIdx;
                    }

                    // TODO: check for required fields?
                } ();
            }

            result.loadItem();
            return result;
        }
    }

    // returns the rows affected
    long perform(Q)(Connection conn, Q stmt) if(is(Q : Insert!P, P) ||
                                                is(Q : Update!P, P))
    {
        import mysql.commands;

        if(stmt.params.empty)
        {
            return conn.exec(stmt.sql);
        }
        else
        {
            import std.range : enumerate;
            auto p = conn.prepare(stmt.sql);
            foreach(idx, arg; stmt.params.enumerate)
                p.setArg(idx, arg);
            return conn.exec(p);
        }
    }

    T create(T)(Connection conn, T blueprint)
    {
        import mysql.commands;
        import std.traits;
        import sqlbuilder.uda;

        // use insert to create it
        auto ins = insert!Variant(blueprint);
        auto rowsAffected = conn.perform(ins);
        // check for an autoInc field
        foreach(fname; __traits(allMembers, T))
            static if(isField!(T, fname) &&
                      hasUDA!(__traits(getMember, T, fname), autoIncrement))
            {
                __traits(getMember, blueprint, fname) =
                     cast(typeof(__traits(getMember, blueprint, fname)))conn.lastInsertID;
            }
        return blueprint;
    }

    unittest
    {
        import sqlbuilder.dataset;
        import mysql.commands;
        import mysql.connection;
        import mysql.protocol.sockets;
        auto conn = new Connection(MySQLSocketType.phobos, "localhost", "test", "test", "test");
        scope(exit) conn.close();
        conn.exec(dropTableSql!Author);
        conn.exec(createTableSql!Author);
        conn.exec(dropTableSql!book);
        conn.exec(createTableSql!book);
        import std.stdio;
        auto steve = conn.create(Author("Steven", "Schveighoffer"));
        auto andrei = conn.create(Author("Andrei", "Alexandrescu"));
        conn.create(book("This Module", steve.id));
        conn.create(book("The D Programming Language", andrei.id));
        auto book3 = conn.create(book("Modern C++ design", andrei.id));
        assert(!conn.perform(update!Variant(book3)));
        book3.title = "Not so Modern C++ Design";
        assert(conn.perform(update!Variant(book3)));
        auto ds = DataSet!Author();
        foreach(auth, book; conn.fetch(select!string(ds.all, ds.books.all).where(ds.lastName, " = ", "Alexandrescu".param)))
        {
            writeln("author: ", auth, ", book: ", book);
        }
    }
}
