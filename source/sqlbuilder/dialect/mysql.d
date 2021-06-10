module sqlbuilder.dialect.mysql;
public import sqlbuilder.dialect.common : where, changed, limit, orderBy, groupBy, exprCol, as, concat, count, ascend, descend, Parameter, simplifyConditions;
import sqlbuilder.dialect.common : SQLImpl;
import sqlbuilder.types;
import sqlbuilder.traits;
import sqlbuilder.util;

import std.variant : Variant;

private Variant toVariant(T)(T val)
{
    import std.typecons : Nullable;
    static if(is(T : Nullable!U, U))
    {
        if(val.isNull)
            return Variant(null);
        return toVariant(val.get);
    }
    else static if(is(typeof(val.dbValue)))
    {
        return toVariant(val.dbValue);
    }
    else static if(is(T == enum))
    {
        import std.traits : OriginalType;
        return Variant(cast(OriginalType!T)val);
    }
    else static if(is(T == bool))
    {
        // store bool as a byte
        return Variant(cast(byte)val);
    }
    else
    {
        return Variant(val);
    }
}

auto param(T)(T val)
{
    import std.range : only;
    return Parameter!Variant(only(val.toVariant));
}

auto optional(T)(T val, bool isValid)
{
    import std.range : only;
    return Parameter!(Variant, true)(only(val.toVariant), isValid);
}

unittest 
{
    auto p = "hello".param;
    static assert(is(getParamType!(typeof(p)) == Variant));

    import std.typecons : Nullable, nullable;
    Nullable!int x;
    auto p2 = x.param;
    static assert(is(getParamType!(typeof(p2)) == Variant));
    assert(p2.params.front.type == typeid(typeof(null)));

    x = 5;
    p2 = x.param;
    assert(p2.params.front.type != typeid(typeof(null)));
    assert(p2.params.front == 5);
}

// alias all the items from the implementation template for variant
alias _impl = SQLImpl!(Variant, param);
static foreach(f; __traits(allMembers, _impl))
    mixin("alias " ~ f ~ " = _impl." ~ f ~ ";");

// define a query that has no types being returned.
alias UntypedQuery = typeof(select());

private void sqlPut(bool includeObjectSeparators, bool includeTableQualifiers, App)(ref App app, ExprString expr)
{
    BitStack andor;
    andor.push(true); // default to and
    foreach(x; expr.data)
        sqlPut!(includeObjectSeparators, includeTableQualifiers)(app, x, andor);
}

private void sqlPut(bool includeObjectSeparators, bool includeTableQualifiers, App)(ref App app, ExprString expr, ref BitStack andor)
{
    foreach(x; expr.data)
        sqlPut!(includeObjectSeparators, includeTableQualifiers)(app, x, andor);
}

private void sqlPut(bool includeObjectSeparators, bool includeTableQualifiers, App)(ref App app, string x)
{
    BitStack andor;
    andor.push(true); // default to and
    sqlPut!(includeObjectSeparators, includeTableQualifiers)(app, x, andor);
}

private void sqlPut(bool includeObjectSeparators, bool includeTableQualifiers, App)(ref App app, string x, ref BitStack andor)
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
    case leftJoin:
        put(app, " LEFT JOIN ");
        break;
    case rightJoin:
        put(app, " RIGHT JOIN ");
        break;
    case innerJoin:
        put(app, " INNER JOIN ");
        break;
    case outerJoin:
        put(app, " OUTER JOIN ");
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
    case separator:
        if(andor.peek)
            put(app, " AND ");
        else
            put(app, " OR ");
        break;
    case beginAnd:
        andor.push(true);
        // 2 parentheses, one for the group, and one for the first term
        put(app, "(");
        break;
    case beginOr:
        andor.push(false);
        put(app, "(");
        break;
    case endGroup:
        if(!andor.length)
            throw new Exception("Error, inconsistent groupings");
        andor.pop;
        put(app, ")");
        break;
    default:
        throw new Exception("Unknown spec in: " ~ x);
    }
}

auto params(T)(T t)
{
    static if(isQuery!T)
    {
        return paramsImpl!("fields", "joins", "conditions", "groups", "orders")(t);
    }
    else static if(is(T : Insert!P, P))
    {
        return paramsImpl!("colNames", "colValues")(t);
    }
    else static if(is(T : Update!P, P))
    {
        return paramsImpl!("joins", "settings", "conditions")(t);
    }
    else static if(is(T : Delete!P, P))
    {
        return paramsImpl!("joins", "conditions")(t);
    }
    else static assert("Unsupported type for params property: " ~ T.stringof);
}

string sql(bool includeObjectSeparators = false, QP...)(Query!(QP) q)
{
    import std.array : Appender;
    import std.range : put;
    import std.format : formattedWrite;
    Appender!string app;
    assert(q.fields.expr);
    assert(q.joins.expr);

    // add a set of fragments given the prefix and the separator
    void addFragment(SQLFragment!(q.ItemType) item, string prefix, string postfix = null)
    {
        BitStack bits;
        bits.push(true);
        if(item.expr)
        {
            put(app, prefix);
            sqlPut!(includeObjectSeparators, true)(app, item.expr, bits);
            if(postfix.length)
                put(app, postfix);
        }
    }

    // fields
    addFragment(q.fields, "SELECT ");
    // joins
    addFragment(q.joins, " FROM ");
    // CONDITIONS
    addFragment(q.conditions, " WHERE (", ")");
    // GROUP BY
    addFragment(q.groups, " GROUP BY ");
    // ORDER BY
    addFragment(q.orders, " ORDER BY ");

    if(q.limitQty)
    {
        if(q.limitOffset)
            formattedWrite(app, " LIMIT %s, %s", q.limitOffset, q.limitQty);
        else
            formattedWrite(app, " LIMIT %s", q.limitQty);
    }

    return app.data;
}

// return an sql statement that queries the total items from the given query
// (no limits, no offsets)
//
// The resulting query can be sent to fetch or fetchOne, and this will get a
// single ulong that counts the total rows.

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
    void addFragment(SQLFragment!(upd.ItemType) item, string prefix, string postfix = null)
    {
        if(item.expr)
        {
            put(app, prefix);
            sqlPut!(false, true)(app, item.expr);
            if(postfix.length)
                put(app, postfix);
        }
    }

    // fields
    addFragment(upd.joins, "UPDATE ");
    // joins
    addFragment(upd.settings, " SET ");
    // CONDITIONS
    addFragment(upd.conditions, " WHERE (", ")");

    return app.data;
}

string sql(Item)(Delete!Item del)
{
    // DELETE FROM table LEFT JOIN other tables.
    import std.array : Appender;
    import std.range : put;
    Appender!string app;
    assert(del.joins.expr);

    // add a set of fragments given the prefix and the separator
    void addFragment(SQLFragment!(del.ItemType) item, string prefix, string postfix = null)
    {
        if(item.expr)
        {
            put(app, prefix);
            sqlPut!(false, true)(app, item.expr);
            if(postfix.length)
                put(app, postfix);
        }
    }

    // if there is at least one join, we have to change the syntax
    import std.algorithm : canFind;
    put(app, "DELETE ");
    if(del.joins.expr.data.canFind!(s => s.getSpec.isJoin))
    {
        // get the first table
        sqlPut!(false, true)(app, ExprString(del.joins.expr.data[0 .. 1]));
    }
    // table to delete from, and any joins.
    addFragment(del.joins, " FROM ");
    // CONDITIONS
    addFragment(del.conditions, " WHERE (", ")");

    return app.data;
}

// TODO: I need to test all these mappings properly and make sure they
// deserialize.
private import std.meta : AliasSeq;
private import std.datetime : Date, DateTime, TimeOfDay;
private alias _typeMappings = AliasSeq!(
    bool, "BOOLEAN", // same as TINYINT
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

private template dbValueType(T)
{
    import std.typecons : Nullable;
    static if(is(T : Nullable!U, U))
        alias dbValueType = .dbValueType!U;
    else static if(is(typeof(T.init.dbValue)))
    {
        alias dbValueType = typeof((){T val = T.init; return val.dbValue;}());
    }
    else static if(is(T == enum))
    {
        import std.traits : OriginalType;
        alias dbValueType = OriginalType!T;
    }
    else
        alias dbValueType = T;
}

// match the D type to a specific MySQL type
private template getFieldType(T)
{
    alias RT = dbValueType!T;
    static if(is(RT == T))
    {
        static foreach(i; 0 .. _typeMappings.length / 2)
            static if(is(T == _typeMappings[i * 2]))
            enum result = _typeMappings[i * 2 + 1];
        static if(is(typeof(result)))
            alias getFieldType = result;
        else
            static assert(0, "Unknown mapping for type " ~ T.stringof);
    }
    else
        alias getFieldType = .getFieldType!RT;
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
            alias fieldType = typeof(__traits(getMember, T, field));
            // Relations are not columns to store in the DB.
            static if(!hasUDA!(__traits(getMember, T, field), ignore) &&
                      !is(fieldType == Relation))
            {
                string name = result ~= "`"
                    ~ getColumnName!(__traits(getMember, T, field)) ~ "` ";
                alias colTypeUDAs = getUDAs!(__traits(getMember, T, field), colType);
                static if(colTypeUDAs.length > 0)
                    result ~= colTypeUDAs[0].type;
                else
                    result ~= getFieldType!fieldType;
                static if(!possibleNullColumn!(__traits(getMember, T, field)))
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
template createRelationsSql(T)
{
    string[] relations()
    {
        import std.traits;
        import std.typecons : Nullable;
        import sqlbuilder.uda;
        import sqlbuilder.traits;
        string[] result;
        foreach(field; FieldNameTuple!T)
        {
            // only look for field relations, not Relation items which have no
            // local field.
            static if(isField!(T, field) && isRelationField!(__traits(getMember, T, field)))
            {
                // get the relation name
                alias mappings = getMappingsFor!(__traits(getMember, T, field));
                enum relation = getRelationFor!(__traits(getMember, T, field));
                string alteration = "ALTER TABLE `" ~ getTableName!T ~ "` ADD FOREIGN KEY (`";
                //static
                foreach(i, m; mappings)
                {
                    static if(i != 0)
                        alteration ~= "`, `";
                    alteration ~= getColumnName!(__traits(getMember, T, m.key));
                }
                alteration ~= "`) REFERENCES `" ~ getTableName!(relation.foreign_table) ~ "` (`";
                foreach(i, m; mappings)
                {
                    static if(i != 0)
                        alteration ~= "`, `";
                    alteration ~= getColumnName!(__traits(getMember, relation.foreign_table, m.foreign_key));
                }
                alteration ~= "`)";
                result ~= alteration;
            }
        }
        return result;
    }
    enum createRelationsSql = relations();
}


import std.typecons : Nullable;
import std.traits : isInstanceOf;
// check if a type is a primitive, vs. an aggregate with individual columns
private template isMysqlPrimitive(T)
{
    alias RT = dbValueType!T;
    enum isMysqlPrimitive = !is(RT == struct) || is(RT == Date)
        || is(RT == DateTime) || is(RT == TimeOfDay);
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
            else static if(isInstanceOf!(Changed, T))
            {
                import std.meta : Repeat;
                alias columnFieldNames = Repeat!(typeof(T.val).length, "");
            }
            else
            {
                import std.meta : Filter;
                enum includeIt(string fieldname) = isField!(T, fieldname);
                alias columnFieldNames = Filter!(includeIt, __traits(allMembers, T));
            }
        }
    }

    private auto getLeaf(T)(Variant v) if (isMysqlPrimitive!T)
    {
        alias RT = dbValueType!T;
        static if(is(T : Nullable!U, U))
        {
            if(v.type == typeid(typeof(null)))
            {
                return T.init;
            }
            return T(getLeaf!U(v));
        }
        else
        {
            // null not tolerated
            import std.conv;
            static if(is(RT == T))
            {
                static if(is(T == bool))
                    // booleans are stored as tiny integers
                    return v.get!byte != 0;
                else
                    return v.get!T;
            }
            else static if(is(typeof(T(RT.init))))
                return T(v.get!RT);
            else static if(is(typeof(T.fromDbValue(RT.init))))
                return T.fromDbValue(v.get!RT);
            else static if(is(typeof(RT.init.to!T)))
                return v.get!RT.to!T;
            else
                static assert(0, "Cannot figure out how to convert database value of type " ~ RT.stringof ~ " to D type " ~ T.stringof);
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
        else static if(isInstanceOf!(Changed, T))
        {
            // specialized version, we do not process by field names, but by
            // the tuple stored in the type.
            T result;
            static foreach(idx; 0 .. T.val.length)
            {
                result.val[idx] = getLeaf!(typeof(result.val[idx]))(r[colIds[idx]]);
            }
            return result;
        }
        else
        {
            T result;
            foreach(idx, n; columnFieldNames!T)
            {
                if(colIds[idx] != size_t.max)
                {
                    import sqlbuilder.uda;
                    alias mem = __traits(getMember, T, n);

                    static foreach(alias att; __traits(getAttributes, mem))
                    {
                        static if(__traits(isSame, att, allowNull))
                            enum nullValue = typeof(mem).init;
                        else static if(is(typeof(att) : AllowNull!U, U))
                            enum nullValue = att.nullValue;

                    }

                    static if(is(typeof(nullValue)))
                    {
                        // use Nullable!X to get the data
                        auto v = getLeaf!(Nullable!(typeof(mem)))(r[colIds[idx]]);
                        __traits(getMember, result, n) = v.get(nullValue);
                    }
                    else
                    {
                        __traits(getMember, result, n) = getLeaf!(typeof(mem))(r[colIds[idx]]);
                    //__traits(getMember, result, n) = getLeaf!(typeof(__traits(getMember, result, n)))(r[colIds[idx]]);
                    }
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
        import std.range;
        scope(failure)
        {
            import std.stdio;
            writeln("Failed SQL is: ", q.sql!true);
        }
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

                private void loadItem(bool isfirst)
                {
                    if(!seq.empty)
                    {
                        auto oldRow = row;
                        static foreach(i; 0 .. row.length)
                        {
                            row[i] = getItem!(q.RowTypes[i])(seq.front,
                                                              colIds[i][]);
                            static if(isInstanceOf!(Changed, typeof(row[i])))
                                // the changed flag needs to be set based on if the previous row is equal to this row

                                row[i]._changed = isfirst || oldRow[i].val != row[i].val;
                        }
                    }
                }

                void popFront()
                {
                    seq.popFront;
                    loadItem(false);
                }
            }

            SerializedRange result;
            result.seq = seq;
            if(seq.empty)
                return result;

            // map all column names to indexes
            size_t colIdx = 0;
            auto colNames = seq.colNames;

            foreach(idx, T; q.RowTypes)
            {
                static if(isMysqlPrimitive!T)
                {
                    assert(colIdx < colNames.length);
                    result.colIds[idx][0] = colIdx;
                    ++colIdx;
                }
                else static if(isInstanceOf!(Changed, T))
                {
                    // always the same number of columns
                    foreach(ref cid; result.colIds[idx])
                    {
                        assert(colIdx < colNames.length);
                        cid = colIdx++;
                    }
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

            result.loadItem(true);
            return result;
        }
    }

    long fetchTotal(QP...)(Connection conn, Query!(QP) q)
    {
        import mysql.result;
        import mysql.commands;
        q.limitQty = 0;
        q.limitOffset = 0;
        q.orders = q.orders.init;
        q.fields = SQLFragment!(q.ItemType)(ExprString("1"));
        auto sql = "SELECT COUNT(*) FROM(" ~ q.sql ~ ") `counter`";
        ResultRange seq;
        if(q.params.empty)
        {
            seq = conn.query(sql);
        }
        else
        {
            auto p = conn.prepare(sql);
            import std.range : enumerate;
            foreach(idx, arg; q.params.enumerate)
                p.setArg(idx, arg);
            seq = conn.query(p);
        }
        return seq.empty ? 0 : seq.front[0].get!long;
    }

    auto fetchOne(bool throwOnExtraColumns = false, QD...)(Connection conn, Query!(QD) q)
    {
        auto r = fetch!throwOnExtraColumns(conn, q);
        import std.range : ElementType;
        if(r.empty)
            throw new Exception("No items of type " ~ ElementType!(typeof(r)).stringof ~ " retreived from query");
        return fetch!throwOnExtraColumns(conn, q).front;
    }

    auto fetchOne(bool throwOnExtraColumns = false, T, QD...)(Connection conn, Query!(QD) q, T defaultValue)
    {
        auto r = fetch!throwOnExtraColumns(conn, q);
        return r.empty ? defaultValue : r.front;
    }

    auto fetchUsingKey(T, bool throwOnExtraColumns = false, Args...)(Connection conn, Args args) if (hasPrimaryKey!T && Args.length == primaryKeyFields!T.length)
    {
        import sqlbuilder.dataset;
        DataSet!T ds;
        return conn.fetchOne!throwOnExtraColumns(select(ds).havingKey(ds, args));
    }

    auto fetchUsingKey(T, bool throwOnExtraColumns = false, Args...)(Connection conn, T defaultValue, Args args) if (hasPrimaryKey!T && Args.length == primaryKeyFields!T.length)
    {
        import sqlbuilder.dataset;
        DataSet!T ds;
        return conn.fetchOne!throwOnExtraColumns(select(ds).havingKey(ds, args), defaultValue);
    }

    // returns the rows affected
    long perform(Q)(Connection conn, Q stmt) if(is(Q : Insert!P, P) ||
                                                is(Q : Update!P, P) ||
                                                is(Q : Delete!P, P))
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

    auto ref T create(T)(Connection conn, auto ref T blueprint)
    {
        import mysql.commands;
        import std.traits;
        import sqlbuilder.uda;

        // use insert to create it
        auto ins = insert(blueprint);
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

    // returns true if the item was updated. Only valid for records that have a
    // primary key.
    bool save(T)(Connection conn, T item) if (hasPrimaryKey!T)
    {
        scope(failure)
        {
            import std.stdio;
            auto upd = update(item);
            writeln(upd.sql);
            writeln(upd.params);
        }
        return conn.perform(update(item)) == 1;
    }

    // returns true if the item was erased from the db.
    bool erase(T)(Connection conn, T item) if (hasPrimaryKey!T)
    {
        return conn.perform(remove(item)) == 1;
    }

    unittest
    {
        import sqlbuilder.dataset;
        import mysql.commands;
        import mysql.connection;
        import mysql.protocol.sockets;
        auto conn = new Connection(MySQLSocketType.phobos, "localhost", "test", "test", "test");
        scope(exit) conn.close();
        conn.exec("SET FOREIGN_KEY_CHECKS = 0");
        conn.exec(dropTableSql!Author);
        conn.exec(dropTableSql!book);
        conn.exec(dropTableSql!review);
        conn.exec("SET FOREIGN_KEY_CHECKS = 1");
        conn.exec(createTableSql!Author);
        conn.exec(createTableSql!book);

        // build the review table manually, to add nulls directly
        import std.stdio;
        static foreach(s; createRelationsSql!Author)
            conn.exec(s);
        static foreach(s; createRelationsSql!book)
        {
            writeln(s);
            conn.exec(s);
        }
        //conn.exec("CREATE TABLE `review` (`book_id` INT NOT NULL, `comment` TEXT, `rating` INT)");
        conn.exec(createTableSql!review);
        auto steve = conn.create(Author("Steven", "Schveighoffer"));
        auto ds = DataSet!Author();
        conn.perform(insert(ds.tableDef).set(ds.firstName, "Andrei".param).set(ds.lastName, Expr(`"Alexandrescu"`)).set(ds.ynAnswer, MyBool(true).param));
        auto andreiId = cast(int)conn.lastInsertID();
        conn.create(book("This Module", steve.id));
        conn.create(book("The D Programming Language", andreiId));
        auto book3 = conn.create(book("Modern C++ design", andreiId));
        assert(!conn.perform(update(book3)));
        book3.title = "Not so Modern C++ Design";
        book3.book_type = BookType.Fiction;
        assert(conn.save(book3));
        foreach(auth, book; conn.fetch(select(ds, ds.books).where(ds.lastName, " = ", "Alexandrescu".param)))
        {
            writeln("author: ", auth, ", book: ", book);
        }

        foreach(newauth, auth, book; conn.fetch(select(ds.changed, ds, ds.books).orderBy(ds.id)))
        {
            if(newauth)
                writeln("Author: ", auth);
            writeln("    book: ", book);
        }

        foreach(newauth, auth, book; conn.fetch(select(ds.id.changed, ds, ds.books).orderBy(ds.id)))
        {
            if(newauth)
                writeln("Author: ", auth);
            writeln("    book: ", book);
        }
        auto book4 = conn.create(book("Remove Me", steve.id));
        writeln(book4);
        assert(conn.erase(book4) == 1);
        DataSet!book ds2;
        //assert(conn.perform(removeFrom(ds2.tableDef).where(ds2.author.lastName, " = ", "Alexandrescu".param)) == 2);
        assert(conn.perform(removeFrom(ds2.tableDef).havingKey(ds2.author, andreiId)) == 2);
        assert(conn.perform(removeFrom(ds2.tableDef).havingKey(ds2.author, steve)) == 1);

        // generate a table with a null value
        import sqlbuilder.uda;
        static struct foo
        {
            @primaryKey int id;
            Nullable!int col1;
        }

        conn.exec(dropTableSql!foo);
        conn.exec(createTableSql!foo);
        auto rds = DataSet!review.init;
        conn.perform(insert(rds.tableDef).set(rds.book_id, 1.param));
        writeln(conn.fetch(select(rds)));
        conn.exec("INSERT INTO foo (id, col1) VALUES (1, ?)", Nullable!int.init);
        auto seq1 = conn.query("SELECT * FROM foo WHERE col1 <=> ?", Variant(null));
        writeln(seq1.colNames);
        writefln("result from query2: %s", conn.query("SELECT * FROM foo WHERE col1 IS NULL"));
    }
}
