module sqlbuilder.dialect.sqlite;

public import sqlbuilder.dialect.common : where, changed, limit, orderBy,
           groupBy, exprCol, as, withoutAs, concat, count, ascend, descend,
           Parameter, simplifyConditions;

private import sqlbuilder.dialect.common : SQLImpl, append;

import sqlbuilder.types;
import sqlbuilder.traits;
import sqlbuilder.util;

import std.typecons : Nullable, nullable;
import std.datetime : DateTime, Date, SysTime;
import std.traits;
import std.conv;

alias Blob = const(ubyte)[];

// define a simple tagged union for the parameters. We don't need anything
// complex, because none of the types are complex.
struct PType
{
    private import std.range;
    enum Tag
    {
        Integer,
        Float,
        Text,
        Blob,
        Null
    }

    private
    {
        Tag _tag;
        union _Value
        {
            long longData; // tag 0
            double doubleData; // tag 1
            string stringData; // tag 2
            Blob blobData; // tag 3
        }
        _Value _value;
    }

    this(T)(T val)
    {
        opAssign(val);
    }

    Tag tag()
    {
        return _tag;
    }

    void opAssign(PType val)
    {
        this._tag = val._tag;
        this._value = val._value;
    }

    void opAssign(T)(T val)
    {
        static if(is(T == Nullable!U, U))
        {
            if(val.isNull)
            {
                _value.blobData = null;
                _tag = Tag.Null;
            }
            else
                opAssign(val.get);
        }
        else static if(is(T == typeof(null)))
        {
            // zero out the union
            _value.blobData = null;
            _tag = Tag.Null;
        }
        else static if(is(T == DateTime) || is(T == SysTime) || is(T == Date))
        {
            _value.stringData = val.toISOExtString;
            _tag = Tag.Text;
        }
        else static if(canStringMarshal!T || isSomeString!T)
        {
            _value.stringData = val.to!string;
            _tag = Tag.Text;
        }
        else static if (isIntegral!T || isSomeChar!T || isBoolean!T)
        {
            _value.longData = val.to!long;
            _tag = Tag.Integer;
        }
        else static if (isFloatingPoint!T)
        {
            _value.doubleData = val;
            _tag = Tag.Float;
        }
        else static if(is(immutable(T) == immutable(Blob)))
        {
            _value.blobData = val;
            _tag = Tag.Blob;
        }
        else
        {
            static assert(false, "Cannot assign type " ~ T.stringof ~ " to PType");
        }
    }

    bool isNull()
    {
        return _tag == Tag.Null;
    }

    T get(T)()
    {
        static if(is(T == Nullable!U, U))
        {
            if(isNull)
                return T.init;
            else
                return T(get!U());
        }
        else
        {
            void enforceTag(Tag required)
            {
                if(_tag != required)
                    throw new Exception("Inappropriate tag type for " ~ T.stringof ~ ": " ~ _tag.to!string);
            }
            static if(canStringMarshal!T || isSomeString!T)
            {
                enforceTag(Tag.Text);
                return parseFromString!T(_value.stringData);
            }
            else static if (isIntegral!T || isSomeChar!T || isBoolean!T)
            {
                enforceTag(Tag.Integer);
                return _value.longData.to!T;
            }
            else static if (isFloatingPoint!T)
            {
                // TODO: should we support integer values also?
                enforceTag(Tag.Float);
                return _value.doubleData;
            }
            else static if(is(T == Blob))
            {
                enforceTag(Tag.Blob);
                return _value.blobData;
            }
            else
                static assert(false, "Cannot get type " ~ T.stringof ~ " from PType");
        }
    }

    void toString(Out)(ref Out output) if (isOutputRange!(Out, dchar))
    {
        import std.format;
        with(Tag) final switch(_tag)
        {
            case Integer:
                formattedWrite(output, "%s", _value.longData);
                break;
            case Float:
                formattedWrite(output, "%s", _value.doubleData);
                break;
            case Text:
                formattedWrite(output, "%s", _value.stringData);
                break;
            case Blob:
                formattedWrite(output, "[%(0x%02x, %)]", _value.blobData);
                break;
            case Null:
                put(output, "null");
                break;
        }
    }

    string toString()
    {
        import std.array;
        Appender!string app;
        toString(app);
        return app.data;
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

alias _impl = SQLImpl!(PType, param, true);

static foreach(f; __traits(allMembers, _impl))
    mixin("alias " ~ f ~ " = _impl." ~ f ~ ";");

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
        put(app, '"');
        put(app, x[2 .. $]);
        put(app, '"');
        break;
    case tableid:
        static if(includeTableQualifiers)
        {
            put(app, '"');
            put(app, x[2 .. $]);
            put(app, `".`);
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
            put(app, `, 1 AS "_objend"`);
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
            formattedWrite(app, " LIMIT %s OFFSET %s", q.limitQty, q.limitOffset);
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

    put(app, `INSERT INTO "`);
    put(app, ins.tableid);
    put(app, `" (`);
    sqlPut!(false, false)(app, ins.colNames.expr);
    put(app, ") VALUES (");
    sqlPut!(false, true)(app, ins.colValues.expr);
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

    // first table
    put(app, "UPDATE ");
    app.sqlPut!(false, true)(upd.joins.expr.data[0]); // add the existing table
    // fields
    addFragment(upd.settings, " SET ");

    // extra joins
    if(upd.joins.expr.data.length > 1)
    {
        import std.algorithm : map;
        // need to build an alternate expression string with "_" as the main table name.
        static immutable string altTableName = "_".makeSpec(Spec.tableid);
        auto altFrag = upd.joins;
        auto origTable = upd.joins.expr.data[0];
        altFrag.expr.data = [origTable, ` AS "_"`];
        altFrag.expr.data.append(upd.joins.expr.data[1 .. $]
                .map!(x => getSpec(x) == Spec.tableid && x[2 .. $] == origTable[2 .. $] ? altTableName : x));
        addFragment(altFrag, " FROM ");
    }
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

    put(app, "DELETE FROM ");
    sqlPut!(false, true)(app, ExprString(del.joins.expr.data[0 .. 1]));
    // if there is at least one join, we have to change the syntax
    import std.algorithm : canFind;
    if(del.joins.expr.data.canFind!(s => s.getSpec.isJoin))
    {
        // there are joins. Use a where select statement to find the rows to
        // delete. Map based on rowids.
        put(app, " WHERE rowid IN (SELECT ");
        sqlPut!(false, true)(app, ExprString(del.joins.expr.data[0 .. 1]));
        put(app, ".rowid ");
        addFragment(del.joins, " FROM ");
        addFragment(del.conditions, " WHERE (", ")");
        put(app, ")");
    }
    else
    {
        // simple delete, no joins.
        addFragment(del.conditions, " WHERE (", ")");
    }

    return app.data;
}

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
        alias dbValueType = OriginalType!T;
    }
    else
        alias dbValueType = T;
}

private enum canStringMarshal(T) =
  (is(typeof(T.init.toString((in char[]) {}))) || is(typeof(T.init.toString())))
    && is(typeof(parseFromString!T));

private T parseFromString(T)(string textData)
{
    static if(isSomeString!T)
        return textData.to!T;
    // handle the timestamp types specially, these are very common DB types.
    // TODO: handle all the time types
    else static if(is(T == DateTime) || is(T == SysTime) || is(T == Date))
        return T.fromISOExtString(textData);
    else static if(is(typeof(T.fromString(textData))))
        return T.fromString(textData);
    else static if(is(typeof(to!T(textData))))
        return textData.to!T;
    else
        static assert(false, "Cannot parse ", T, " from a string");
}

// match the D type to a specific MySQL type
private template getFieldType(T)
{
    alias RT = dbValueType!T;
    static if(is(RT == T))
    {
        static if (isIntegral!T || isSomeChar!T || isBoolean!T)
            enum getFieldType = "INTEGER";
        else static if(canStringMarshal!T || isSomeString!T)
            enum getFieldType = "TEXT";
        else static if(isFloatingPoint!T)
            enum getFieldType = "REAL";
        else static if(is(immutable(T) == immutable(Blob)))
            enum getFieldType = "BLOB";
        else
            static assert(0, "Unknown mapping for type " ~ T.stringof);
    }
    else
        alias getFieldType = .getFieldType!RT;
}


// generate an SQL statement to insert a table definition.
template createTableSql(T, bool doForeignKeys = false, bool ifNotExists = false)
{
    string generate()
    {
        import sqlbuilder.uda;
        import sqlbuilder.traits;
        auto result = `CREATE TABLE ` ~ (ifNotExists ? `IF NOT EXISTS ` : ``) ~
            `"` ~ getTableName!T ~ `" (`;
        int autoIncFields = 0;
        foreach(field; FieldNameTuple!T)
        {
            alias fieldType = typeof(__traits(getMember, T, field));
            // Relations are not columns to store in the DB.
            static if(!hasUDA!(__traits(getMember, T, field), ignore) &&
                      !is(fieldType == Relation))
            {
                string name = result ~= `"`
                    ~ getColumnName!(__traits(getMember, T, field)) ~ `" `;
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
                {
                    static assert(hasUDA!(__traits(getMember, T, field), primaryKey));
                    result ~= " PRIMARY KEY AUTOINCREMENT";
                    if(++autoIncFields > 1)
                    {
                        assert(0, "Auto increment only allowed on one field!");
                    }
                }

                result ~= ",";
            }
        }
        // add primary key
        alias keyFields = getSymbolsByUDA!(T, primaryKey);
        static if(keyFields.length > 0)
        {
            if(autoIncFields > 0)
            {
                assert(keyFields.length == 1, "Only one primary key allowed when autoIncrement is used");
            }
            else
            {
                result ~= " PRIMARY KEY (";
                static foreach(i, alias kf; keyFields)
                {
                    static if(i != 0)
                        result ~= ",";
                    result ~= `"` ~ getColumnName!(kf) ~ `"`;
                }
                result ~= "),";
            }
        }

        static if(doForeignKeys)
        {
            import sqlbuilder.uda;
            import sqlbuilder.traits;
            foreach(field; FieldNameTuple!T)
            {
                // only look for field relations, not Relation items which have no
                // local field.
                static if(isField!(T, field) && isRelationField!(__traits(getMember, T, field)))
                {
                    // get the relation name
                    alias mappings = getMappingsFor!(__traits(getMember, T, field));
                    enum relation = getRelationFor!(__traits(getMember, T, field));
                    result ~= `FOREIGN KEY ("`;
                    foreach(i, m; mappings)
                    {
                        static if(i != 0)
                            result ~= `", "`;
                        result ~= getColumnName!(__traits(getMember, T, m.key));
                    }
                    result ~= `") REFERENCES "` ~ getTableName!(relation.foreign_table) ~ `" ("`;
                    foreach(i, m; mappings)
                    {
                        static if(i != 0)
                            result ~= `", "`;
                        result ~= getColumnName!(__traits(getMember, relation.foreign_table, m.foreign_key));
                    }
                    result ~= `"),`;
                }
            }
        }

        return result[0 .. $-1] ~ ")";
    }
    enum createTableSql = generate();
}

enum dropTableSql(T) = `DROP TABLE IF EXISTS "` ~ getTableName!T ~ `"`;

// if we have sqlite as a dependency, provide direct serialization from a ResultRange
version(Have_d2sqlite3)
{
    private import sqlbuilder.dialect.impl : objFieldNames;
    private import d2sqlite3: Row, SqliteType, ResultRange, Database, Statement;

    PType getPType(Row r, size_t idx)
    {
        PType result;
        final switch(r.columnType(idx))
        {
            case SqliteType.INTEGER:
                result = r.peek!long(idx);
                break;
            case SqliteType.FLOAT:
                result = r.peek!double(idx);
                break;
            case SqliteType.TEXT:
                result = r.peek!string(idx);
                break;
            case SqliteType.BLOB:
                result = r.peek!(immutable(ubyte)[])(idx);
                break;
            case SqliteType.NULL:
                result = null;
                break;
        }
        return result;
    }

    private auto getLeaf(T)(PType v)
    {
        static if(is(T : Nullable!U, U))
        {
            if(v.isNull)
            {
                return T.init;
            }
            return T(getLeaf!U(v));
        }
        else static if(is(T == AllowNullType!Args, Args...))
        {
            if(v.isNull)
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
            static if(is(RT == T))
                return v.get!T;
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

    struct DefaultObjectDeserializer(T)
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
        size_t[objFieldNames!RT.length] colIds = size_t.max;
        // returns true if we know about this columm otherwise false.
        bool mapColumnId(string colname, size_t idx)
        {
objSwitch:
            switch(colname)
            {
                static foreach(fnum, fname; objFieldNames!RT)
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
            static foreach(fnum, fname; objFieldNames!RT)
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
            foreach(idx, n; objFieldNames!RT)
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
                        auto v = getLeaf!(Nullable!(typeof(mem)))(r.getPType(colIds[idx]));
                        __traits(getMember, result, n) = v.get(nullValue);
                    }
                    else static if(isNullType && !(is(typeof(mem) == Nullable!N, N)))
                    {
                        // get as nullable, then set the whole object
                        // to null if it is null.
                        static if(is(typeof(mem) == void))
                            pragma(msg, n);
                        auto v = getLeaf!(Nullable!(typeof(mem)))(r.getPType(colIds[idx]));
                        if(v.isNull)
                        {
                            wholeObjNull = true;
                            break;
                        }
                        else
                            __traits(getMember, result, n) = v.get;
                    }
                    else
                    {
                        __traits(getMember, result, n) = getLeaf!(typeof(mem))(r.getPType(colIds[idx]));
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
            if(colId == size_t.max)
            {
                // ignore the name.
                colId = idx;
                return true;
            }
            // only have one column
            return false;
        }

        auto deserializeRow(Row r)
        {
            return getLeaf!T(r.getPType(colId));
        }
    }

    struct ChangeDeserializer(T)
    {
        static if(is(T == Changed!(ColTypes), ColTypes...))
            alias CT = ColTypes;
        else
            static assert(0, "ChangeDeserializer must only be used with a Changed type, not ", T);
        size_t[CT.length] colIds = 0;
        size_t filled = 0;
        bool mapColumnId(string colname, size_t idx)
        {
            if(filled == colIds.length)
                return false;
            colIds[filled++] = idx;
            return true;
        }

        auto deserializeRow(Row r)
        {
            T result;
            static foreach(idx, U; CT)
                result.val[idx] = getLeaf!U(r.getPType(colIds[idx]));
            return result;
        }
    }

    private void setArg(Statement p, size_t idx, PType arg)
    {
        // note sqlite parameter indexes are 1-based
        int idxi = cast(int)idx + 1;
        with(PType.Tag) final switch(arg.tag)
        {
            case Integer:
                p.bind(idxi, arg.get!long);
                break;
            case Float:
                p.bind(idxi, arg.get!double);
                break;
            case Text:
                // work around null pointer bug.
                // See https://github.com/dlang-community/d2sqlite3/issues/77
                auto str = arg.get!string;
                if(str.ptr is null)
                    str = "";
                p.bind(idxi, str);
                break;
            case Blob:
                // work around null pointer bug.
                // See https://github.com/dlang-community/d2sqlite3/issues/77
                auto data = arg.get!(.Blob);
                if(data.ptr is null)
                    data = (data.ptr + 1)[0 .. 0];
                p.bind(idxi, data);
                break;
            case Null:
                p.bind(idxi, null);
                break;
        }
    }

    // fetch a range of serialized items
    auto fetch(bool throwOnExtraColumns = false, QD...)(Database conn, Query!(QD) q)
    {
        import std.range;
        scope(failure)
        {
            import std.stdio;
            writeln("Failed SQL is: ", q.sql!true);
        }
        auto p = conn.prepare(q.sql!true);
        import std.range : enumerate;
        foreach(idx, arg; q.params.enumerate)
            p.setArg(idx, arg);
        auto seq = p.execute();

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
                    static if(__traits(hasMember, U, "initialColumnIds"))
                        static assert(0, "initialColumnIds is no longer the correct way to hook custom deserialization. please use sqlbuilderDeserializer.");
                    static if(__traits(hasMember, U, "sqlbuilderDeserializer"))
                        alias getDeserializer = typeof(() {return U.sqlbuilderDeserializer;}());
                    else
                        alias getDeserializer = DefaultObjectDeserializer!U;
                }
                else static if(is(T == Changed!(Args), Args...))
                    alias getDeserializer = ChangeDeserializer!T;
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
                        auto sqlRow = seq.front;
                        static foreach(i; 0 .. row.length)
                        {
                            row[i] = deserializers[i].deserializeRow(sqlRow);
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

            string[] colNames = new string[seq.front.length];
            foreach(i; 0 .. seq.front.length)
                colNames[i] = seq.front.columnName(i);
            size_t colIdx;

            foreach(idx, T; q.QueryTypes)
            {
                static if(is(T == RowObj!U, U))
                {
                    // handle this differently, because this could have a
                    // custom deserializer, and we want to avoid passing in the
                    // object end marker.
                    static if(__traits(hasMember, U, "sqlbuilderDeserializer"))
                        result.deserializers[idx] = U.sqlbuilderDeserializer;
                    // serialize columns until we hit _objEnd
                    while(colNames[colIdx] != "_objend")
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
                    while(colIdx < colNames.length &&
                            result.deserializers[idx].mapColumnId(colNames[colIdx], colIdx))
                        ++colIdx;
                }
            }

            result.loadItem(true);
            return result;
        }
    }

    long fetchTotal(QP...)(Database conn, Query!(QP) q)
    {
        q.limitQty = 0;
        q.limitOffset = 0;
        q.orders = q.orders.init;
        q.fields = SQLFragment!(q.ItemType)(ExprString("1"));
        auto sql = "SELECT COUNT(*) FROM(" ~ q.sql ~ ") `counter`";
        ResultRange seq;
        auto p = conn.prepare(sql);
        import std.range : enumerate;
        foreach(idx, arg; q.params.enumerate)
            p.setArg(idx, arg);
        seq = p.execute();
        return seq.empty ? 0 : seq.front.peek!long(0);
    }

    auto fetchOne(bool throwOnExtraColumns = false, QD...)(Database conn, Query!(QD) q)
    {
        auto r = fetch!throwOnExtraColumns(conn, q.limit(1));
        import std.range : ElementType;
        if(r.empty)
            throw new Exception("No items of type " ~ ElementType!(typeof(r)).stringof ~ " retreived from query");
        return r.front;
    }

    auto fetchOne(bool throwOnExtraColumns = false, T, QD...)(Database conn, Query!(QD) q, T defaultValue)
    {
        auto r = fetch!throwOnExtraColumns(conn, q.limit(1));
        return r.empty ? defaultValue : r.front;
    }

    template fetchUsingKey(T, bool throwOnExtraColumns = false) if (hasPrimaryKey!T)
    {
        auto fetchUsingKey(Args...)(Database conn, Args args) if (Args.length == primaryKeyFields!T.length)
        {
            import sqlbuilder.dataset;
            DataSet!T ds;
            return conn.fetchOne!throwOnExtraColumns(select(ds).havingKey(ds, args));
        }
    }

    auto fetchUsingKey(T, bool throwOnExtraColumns = false, Args...)(Database conn, T defaultValue, Args args) if (hasPrimaryKey!T && Args.length == primaryKeyFields!T.length)
    {
        import sqlbuilder.dataset;
        DataSet!T ds;
        return conn.fetchOne!throwOnExtraColumns(select(ds).havingKey(ds, args), defaultValue);
    }

    // returns the rows affected
    long perform(Q)(Database conn, Q stmt) if(is(Q : Insert!P, P) ||
                                                is(Q : Update!P, P) ||
                                                is(Q : Delete!P, P))
    {
        import std.range : enumerate;
        import std.stdio;
        scope(failure) writeln("failed statement is ", stmt.sql, " fields are ", stmt.params);
        auto p = conn.prepare(stmt.sql);
        foreach(idx, arg; stmt.params.enumerate)
            p.setArg(idx, arg);
        auto result = p.execute;
        return conn.changes();
    }

    auto ref T create(T)(Database conn, auto ref T blueprint)
    {
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
                     cast(typeof(__traits(getMember, blueprint, fname)))conn.lastInsertRowid;
            }
        return blueprint;
    }

    // returns true if the item was present. Only valid for records that have a
    // primary key.
    bool save(T)(Database conn, T item) if (hasPrimaryKey!T)
    {
        return conn.perform(update(item)) == 1;
    }

    // returns true if the item was erased from the db.
    bool erase(T)(Database conn, T item) if (hasPrimaryKey!T)
    {
        return conn.perform(remove(item)) == 1;
    }

    unittest
    {
        import sqlbuilder.dataset;
        import std.stdio;
        import std.range;
        import std.algorithm;
        static import std.file;
        auto tmpfilename = "./testDB.sqlite";
        if(std.file.exists(tmpfilename))
            std.file.remove(tmpfilename);
        auto conn = Database(tmpfilename);

        conn.execute(createTableSql!(Author, true));
        conn.execute(createTableSql!(book, true));

        conn.execute(createTableSql!(review, true));
        auto steve = conn.create(Author("Steven", "Schveighoffer"));
        auto ds = DataSet!Author();
        conn.perform(insert(ds.tableDef).set(ds.firstName, "Andrei".param).set(ds.lastName, Expr(`"Alexandrescu"`)).set(ds.ynAnswer, MyBool(true).param));
        auto andreiId = cast(int)conn.lastInsertRowid();
        conn.create(book("This Module", steve.id));
        conn.create(book("The D Programming Language", andreiId));
        auto book3 = conn.create(book("Modern C++ design", andreiId));
        // always a change if we update, as long as the book is found.
        assert(conn.perform(update(book3)) == 1);
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

        conn.execute(createTableSql!foo);
        auto rds = DataSet!review.init;
        conn.perform(insert(rds.tableDef).set(rds.book_id, 1.param));
        writeln(conn.fetch(select(rds)));

        // test allowNull columns
        auto nullrating = conn.fetchOne(select(rds.rating).where(rds.book_id, " = ", 1.param));
        assert(nullrating == -1);

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
                bool mapColumnId(string colname, size_t idx)
                {
                    if(startColId == -1)
                        startColId = idx;
                    endColId = idx + 1;
                    names ~= colname;
                    return true;
                }

                CustomObj deserializeRow(Row r)
                {
                    import std.array;
                    CustomObj result;
                    result.colnames = names;
                    result.things = zip(cycle(only(r)), iota(startColId, endColId)).map!(t => t[0].getPType(t[1])).array;
                    return result;
                }
            }
            enum sqlbuilderDeserializer = CustomObjSerializer.init;
        }

        // fetch the custom row from the table
        auto customrow = ColumnDef!(RowObj!CustomObj)(TableDef("author", ExprString("author".makeSpec(Spec.id))), ExprString("*", objEndSpec));
        auto query3 = select(customrow);
        writeln("query3 is ", query3.sql);
        foreach(co; conn.fetch(select(customrow)))
        {
            static assert(is(typeof(co) == CustomObj));
            writefln("Got author row (column names = %-(%s, %)): %s", co.colnames, co.things);
        }
    }
}
