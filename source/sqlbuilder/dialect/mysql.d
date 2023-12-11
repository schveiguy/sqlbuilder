module sqlbuilder.dialect.mysql;
public import sqlbuilder.dialect.common : where, changed, limit, orderBy,
           groupBy, exprCol, as, withoutAs, concat, count, ascend, descend,
           Parameter, simplifyConditions;
import sqlbuilder.dialect.common : SQLImpl;
import sqlbuilder.types;
import sqlbuilder.traits;
import sqlbuilder.util;

import std.typecons : Nullable, nullable;

// determine whether we can use safe mode.
static if(__traits(compiles, {import mysql.safe.commands;}))
{
    version = MysqlHaveSafe;
    import mysql.types : PType = MySQLVal, get;
    private string _ip(string s)
    {
        return "import mysql.safe." ~ s ~ ";";
    }
    @safe pure nothrow @nogc private bool isNullVal(PType t)
    {
        return t.kind == t.Kind.Null;
    }
}
else
{
    import std.variant : PType = Variant;
    private string _ip(string s) {
        return "import mysql." ~ s ~ ";";
    }
    private bool isNullVal(PType t)
    {
        return t.type == typeid(typeof(null));
    }
}

private PType toPType(T)(T val)
{
    static if(is(T : Nullable!U, U))
    {
        if(val.isNull)
            return PType(null);
        return toPType(val.get);
    }
    else static if(is(typeof(val.dbValue)))
    {
        return toPType(val.dbValue);
    }
    else static if(is(T == enum))
    {
        import std.traits : OriginalType;
        return toPType(cast(OriginalType!T)val);
    }
    else static if(is(T == bool))
    {
        // store bool as a byte
        return toPType(cast(byte)val);
    }
    else
    {
        return PType(val);
    }
}

auto param(T)(T val)
{
    import std.range : only;
    return Parameter!PType(only(val.toPType));
}

auto optional(T)(T val, bool isValid)
{
    import std.range : only;
    return Parameter!(PType, true)(only(val.toPType), isValid);
}

unittest 
{
    version(all)
    {
        auto p = "hello".param;
        static assert(is(getParamType!(typeof(p)) == PType));

        Nullable!int x;
        auto p2 = x.param;
        static assert(is(getParamType!(typeof(p2)) == PType));
        assert(isNullVal(p2.params.front));

        x = 5;
        p2 = x.param;
        assert(!isNullVal(p2.params.front));
        assert(p2.params.front == 5);
    }
    else
    {
        auto p = "hello".param;
        static assert(is(getParamType!(typeof(p)) == PType));

        Nullable!int x;
        auto p2 = x.param;
        static assert(is(getParamType!(typeof(p2)) == PType));
        assert(p2.params.front.type == typeid(typeof(null)));

        x = 5;
        p2 = x.param;
        assert(p2.params.front.type != typeid(typeof(null)));
        assert(p2.params.front == 5);
    }
}

// alias all the items from the implementation template for the appropriate parameter type
alias _impl = SQLImpl!(PType, param);

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
            put(app, ") AND (");
        else
            put(app, ") OR (");
        break;
    case beginAnd:
        andor.push(true);
        // 2 parentheses, one for the group, and one for the first term
        put(app, "((");
        break;
    case beginOr:
        andor.push(false);
        put(app, "((");
        break;
    case endGroup:
        if(!andor.length)
            throw new Exception("Error, inconsistent groupings");
        andor.pop;
        put(app, "))");
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
    static if(is(T : Nullable!U, U))
        alias dbValueType = .dbValueType!U;
    else static if(is(T : AllowNullType!Args, Args...))
        alias dbValueType = .dbValueType!(T.type);
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


// check if a type is a primitive, vs. an aggregate with individual columns
private template isMysqlPrimitive(T)
{
    alias RT = dbValueType!T;
    enum isMysqlPrimitive = !is(RT == struct) || is(RT == Date)
        || is(RT == DateTime) || is(RT == TimeOfDay) || is(RT == PType);
}

// dialect-agnostic way to determine if a column is the object end sentinel
bool isObjEnd(string colname) @safe pure @nogc nothrow
{
    return colname == "_objend";
}

// if we have mysql native as a dependency, provide direct serialization from a ResultRange
version(Have_mysql_native)
{

    mixin(_ip("result : Row"));
    import std.traits : isInstanceOf;
    private template columnFieldNames(T)
    {
        static if(isMysqlPrimitive!T)
            // signal we have a column, but it has no fields
            alias columnFieldNames = AliasSeq!("this");
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

    private auto getLeaf(T)(PType v) if (isMysqlPrimitive!T)
    {
        static if(is(T : Nullable!U, U))
        {
            if(v.isNullVal)
            {
                return T.init;
            }
            return T(getLeaf!U(v));
        }
        else static if(is(T == AllowNullType!Args, Args...))
        {
            if(v.isNullVal)
                return T.nullVal;
            return getLeaf!(T.type)(v);
        }
        else static if(is(T == PType))
        {
            // just return as-is
            return v;
        }
        else
        {
            alias RT = dbValueType!T;
            // null not tolerated
            import std.conv;
            static if(is(RT == T))
            {
                static if(is(T == bool))
                    // booleans are stored as tiny integers, signed or unsigned
                    return v.get!byte != 0;
                else
                    return v.get!T;
            }
            else static if(is(typeof(T.fromDbValue(RT.init))))
                return T.fromDbValue(v.get!RT);
            else static if(is(typeof(T(RT.init))))
                return T(v.get!RT);
            else static if(is(typeof(RT.init.to!T)))
                return v.get!RT.to!T;
            else
                static assert(0, "Cannot figure out how to convert database value of type " ~ RT.stringof ~ " to D type " ~ T.stringof);
        }
    }


    struct DefaultObjectDeserialiezr(T)
    {
        static if(is(T == Nullable!U, U))
        {
            private alias RT = U;
            private enum isNullType = true;
        }
        else
        {
            private alias RT = T;
            private enum isNullType = false;
        }
        size_t[columnFieldNames!RT.length] colIds = size_t.max;
        // returns true if we know about this columm otherwise false.
        bool mapColumnId(string colname, size_t idx)
        {
objSwitch:
            switch(colname)
            {
                static foreach(fnum, fname; columnFieldNames!RT)
                {
                    case getColumnName!(__traits(getMember, RT, fname)):
                        colIds[fnum] = idx;
                        return true;
                }
                default:
                    // unhandled.
                    return false;
            }
        }
        string[] unmappedColumns()
        {
            string[] result;
            static foreach(fnum, fname; columnFieldNames!RT)
            {
                if(colIds[fnum] == size_t.max)
                    result ~= fname;
            }
            return result;
        }

        auto deserializeRow(Row r)
        {
            // deserialize all the data, but if any are null that can't be,
            // make the whole thing null.
            RT result;
            static if(isNullType)
                bool wholeObjNull = false;
            bool wholeObjNull = false;
            foreach(idx, n; columnFieldNames!RT)
            {
                if(colIds[idx] != size_t.max)
                {
                    import sqlbuilder.uda;
                    alias mem = __traits(getMember, RT, n);

                    // TODO: figure out to merge this code with the specific
                    // AllowNullType code.
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
                    else static if(isNullType && !(is(typeof(mem) == Nullable!N, N)))
                    {
                        // get as nullable, then set the whole object
                        // to null if it is null.
                        auto v = getLeaf!(Nullable!(typeof(mem)))(r[colIds[idx]]);
                        if(v.isNull)
                        {
                            wholeObjNull = true;
                            break;
                        }
                    }
                    else
                    {
                        __traits(getMember, result, n) = getLeaf!(typeof(mem))(r[colIds[idx]]);
                    }
                }
            }

            static if(isNullType)
            {
                if(wholeObjNull)
                    return Nullable!RT.init; // return a null value
                return result.nullable;
            }
            else
                return result;
        }
    }

    struct NonObjectDeserializer(T)
    {
        size_t colId = size_t.max;
        bool mapColumnId(string colname, size_t idx)
        {
            if(colIds == size_t.max)
            {
                // ignore the name.
                colIds = idx;
                return true;
            }
            // only have one column
            return false;
        }

        auto deserializeRow(Row r)
        {
            return getLeaf!T(r[colId]);
        }
    }

    struct ChangeDeserializer(T)
    {
        static if(is(T == Changed(ColTypes), ColTypes...))
            alias CT = ColTypes;
        else
            static assert(0, "ChangeDeserializer must only be used with a Changed type, not ", T);
        size_t[CT.length] colIds = 0;
        size_t filled = 0;
        bool mapColumnId(string colname, size_t idx)
        {
            if(filled == colIdx.length)
                return false;
            colIds[filled++] = idx;
            return true;
        }

        auto deserializeRow(Row r)
        {
            T result;
            static foreach(idx, U; 0 .. CT.length)
                result.val[idx] = getLeaf!U(r[colIds[idx]]);
            return result;
        }
    }

    private auto getItem(T, IDs)(Row r, ref IDs colIds)
    {
        static if(is(T == RowObj!U, U))
        {
            // this is a row object, and made up of individual columns.
            static if(is(U == Nullable!V, V))
            {
                // each field might be null. If any of them are null, then the
                // whole thing is null.
                enum isNullableType = true;
                alias RT = V;
                bool wholeObjNull = false;
            }
            else
            {
                enum isNullableType = false;
                alias RT = U;
            }
            static if(__traits(hasMember, RT, "deserializeRow"))
            {
                static if(isNullableType)
                {
                    static if(__traits(hasMember, RT, "deserializeRowNull"))
                    {
                        return RT.deserializeRowNull(r, colIds);
                    }
                    else
                    {
                        // try to deserialize without a null value, catch the exception
                        try
                        {
                            RT result = RT.deserializeRow(r, colIds);
                            return result.nullable;
                        }
                        catch(Exception e)
                        {
                            // just return nullable
                            return U.init;
                        }
                    }
                }
                else
                {
                    return RT.deserializeRow(r, colIds);
                }
            }
            else
            {
            }
        }
        else static if(isMysqlPrimitive!T)
        {
            return getLeaf!T(r[colIds[0]]);
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
            static assert(0, "Don't know how to deserialize ", T);
        }
    }

    mixin(_ip("connection"));

    // fetch a range of serialized items
    auto fetch(bool throwOnExtraColumns = false, QD...)(Connection conn, Query!(QD) q)
    {
        mixin(_ip("commands"));
        mixin(_ip("result"));
        import mysql.exceptions;
        ResultRange seq;
        import std.range;
        scope(failure)
        {
            import std.stdio;
            writeln("Failed SQL is: ", q.sql!true);
        }
        bool triedOnce = false;
retryQuery:
        try {
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
        }
        catch(MYXStaleConnection st)
        {
            // connection was stale, retry the connection
            assert(!triedOnce, "Multiple tries on stale connection!");
            triedOnce = true;
            goto retryQuery;
        }

        static if(q.QueryTypes.length == 0 ||  is(q.QueryTypes[0] == void))
        {
            // just return the range generated from the query, types weren't provided.
            return seq;
        }
        else
        {
            // do deserialization.
            // custom types can define a sqlbuilderDeserializer static member
            // to initialize a custom deserializer.
            template getDeserializer(T) {
                static if(is(T == RowObj!U, U))
                {
                    else static if(__traits(hasMember, T, "sqlbuilderDeserializer"))
                        alias getDeserializer = typeof(() {return T.sqlbuilderDeserializer;}());
                    else
                        alias getDeserializer = DefaultObjectDeserializer!U;
                }
                else static if(is(T == Changed!(Args...), Args))
                    alias getDeserializer = ChangedDeserializer!T;
                else
                    alias getDeserializer = NonObjectDeserializer!T;
            }

            import std.meta : staticMap;
            alias colT = staticMap!(getDeserializer, q.QueryTypes);
            static struct SerializedRange
            {
                private ResultRange seq;
                private q.RowTypes row; // here is where we store the front element
                private colT deserializers;

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
                            row[i] = deserializers[i].deserializeRow(seq.front);
                            static if(isInstanceOf!(Changed, typeof(row[i])))
                                // the changed flag needs to be set based on if
                                // the previous row is equal to this row

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

            foreach(idx, T; q.QueryTypes)
            {
                static if(is(T == RowObj!U, U))
                {
                    // handle this differently, because this could have a
                    // custom deserializer, and we want to avoid passing in the
                    // object end marker.
                    static if(__traits(hasMember, U, "sqlbuilderDeserializer"))
                        result.colIds[idx] = U.sqlbuilderDeserializer;
                    // serialize columns until we hit _objEnd
                    while(!isObjEnd(colNames[colIdx]))
                    {
                        if(!result.deserializers[idx].mapColumnId(colNames[colIdx], colIdx))
                        {
                            static if(throwOnExtraColumns)
                                throw new Exception("Unknown column name found: " ~ colNames[colIdx]);
                        }
                        ++colIdx;
                    }
                    ++colIdx; // skip the object end.
                }
                else
                {
                    // Let the deserializer decide how many columns to accept
                    while(result.deserializers[idx].mapColumnId(colNames[colIdx], colIdx))
                        ++colIdx;
                }
            }

            result.loadItem(true);
            return result;
        }
    }

    long fetchTotal(QP...)(Connection conn, Query!(QP) q)
    {
        mixin(_ip("result"));
        mixin(_ip("commands"));
        import mysql.exceptions;
        q.limitQty = 0;
        q.limitOffset = 0;
        q.orders = q.orders.init;
        q.fields = SQLFragment!(q.ItemType)(ExprString("1"));
        auto sql = "SELECT COUNT(*) FROM(" ~ q.sql ~ ") `counter`";
        ResultRange seq;
        bool triedOnce = false;
retryFetchTotal:
        try {

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
        }
        catch(MYXStaleConnection st)
        {
            assert(!triedOnce, "Multiple tries on stale connection!");
            triedOnce = true;
            goto retryFetchTotal;
        }
        return seq.empty ? 0 : seq.front[0].get!long;
    }

    auto fetchOne(bool throwOnExtraColumns = false, QD...)(Connection conn, Query!(QD) q)
    {
        auto r = fetch!throwOnExtraColumns(conn, q.limit(1));
        import std.range : ElementType;
        if(r.empty)
            throw new Exception("No items of type " ~ ElementType!(typeof(r)).stringof ~ " retreived from query");
        return r.front;
    }

    auto fetchOne(bool throwOnExtraColumns = false, T, QD...)(Connection conn, Query!(QD) q, T defaultValue)
    {
        auto r = fetch!throwOnExtraColumns(conn, q.limit(1));
        return r.empty ? defaultValue : r.front;
    }

    template fetchUsingKey(T, bool throwOnExtraColumns = false) if (hasPrimaryKey!T)
    {
        auto fetchUsingKey(Args...)(Connection conn, Args args) if (Args.length == primaryKeyFields!T.length)
        {
            import sqlbuilder.dataset;
            DataSet!T ds;
            return conn.fetchOne!throwOnExtraColumns(select(ds).havingKey(ds, args));
        }
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
        mixin(_ip("commands"));
        import mysql.exceptions;

        bool triedOnce = false;
retryExec:
        try
        {
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
        catch(MYXStaleConnection st)
        {
            assert(!triedOnce, "Multiple tries on stale connection!");
            triedOnce = true;
            goto retryExec;
        }
    }

    auto ref T create(T)(Connection conn, auto ref T blueprint)
    {
        mixin(_ip("commands"));
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
        mixin(_ip("commands"));
        mixin(_ip("connection"));
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

        // test allowNull columns
        auto nullrating = conn.fetchOne(select(rds.rating).where(rds.book_id, " = ", 1.param));
        assert(nullrating == -1);

        conn.exec("INSERT INTO foo (id, col1) VALUES (1, ?)", Nullable!int.init);
        auto seq1 = conn.query("SELECT * FROM foo WHERE col1 <=> ?", PType(null));
        writeln(seq1.colNames);
        writefln("result from query2: %s", conn.query("SELECT * FROM foo WHERE col1 IS NULL"));

        // try a custom object
        static struct CustomObj
        {
            PType[] things;
            string[] colnames;

            // custom serialization
            struct CustomObjSerializer {
                size_t startColId = -1;
                size_t endColId = -1;
                string[] names;
                static bool mapColumnId(string colname, size_t idx)
                {
                    if(startColId == -1)
                        startColId = idx;
                    endColId = idx + 1;
                    names ~= colname;
                    return true;
                }

                CustomObj deserializeRow(Row r)
                {
                    import std.range;
                    import std.algorithm;
                    import std.array;
                    CustomObj result;
                    result.colnames = names;
                    result.things = iota(startColId, endColId).map!(id => r[id]).array;
                    return result;
                }
            }
            enum sqlbuilderDeserializer = CustomObjSerializer.init;
        }

        // fetch the custom row from the table
        auto customrow = ColumnDef!CustomObj(TableDef("author", ExprString("author".makeSpec(Spec.id))), ExprString("*", objEndSpec));
        auto query3 = select(customrow);
        writeln("query3 is ", query3.sql);
        foreach(co; conn.fetch(select(customrow)))
        {
            static assert(is(typeof(co) == CustomObj));
            writefln("Got author row (column names = %-(%s, %)): %s", co.colnames, co.things);
        }
    }
}
