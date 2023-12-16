module sqlbuilder.dialect.sqlite;
/+version(none):

public import sqlbuilder.dialect.common : where, changed, limit, orderBy,
           groupBy, exprCol, as, withoutAs, concat, count, ascend, descend,
           Parameter, simplifyConditions;

import sqlbuilder.dialect.common : SQLImpl;

import sqlbuilder.types;
import sqlbuilder.traits;
import sqlbuilder.util;

import std.typecons : Nullable, nullable;

// define a simple tagged union for the parameters. We don't need anything
// complex, because none of the types are complex.

struct PType
{
    prviate
    {
        size_t _tag;
        union _Value;
        {
            long longData; // tag 0
            double doubleData; // tag 1
            string stringData; // tag 2
            Blob blobData; // tag 3
            typeof(null) nullData; // tag 4
        }
        _Value _value;
    }

    void opAssign(T)(T val)
    {
        static if(is(T == Nullable!U, U))
        {
            if(val.isNull)
            {
                _value.nullData = null;
                _tag = 4;
            }
            else
                opAssign(val.get);
        }
        else static if(is(T == typeof(null)))
        {
            _value.nullData = null;
            _tag = 4;
        }
        else static if(canStringMarshal!T || isSomeString!T)
        {
            _value.stringdata = val.to!string;
            _tag = 2;
        }
        else static if (isIntegral!T || isSomeChar!T || isBoolean!T)
        {
            _value.longData = val.to!long;
            _tag = 0;
        }
        else static if (isFloatingPoint!T)
        {
            _value.doubleData = val;
            _tag = 1;
        }
        else static if(isDynamicArray!T)
        {
            _value.blobData = val;
            _tag = 3;
        }
        else
        {
            static assert(false, "Cannot assign type " ~ T.stringof ~ " to PType");
        }
    }
}

private PType toPType(T)(T val)
{
    PType result;
    result = val;
    return result;
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

alias _impl = SQLImpl!(PType, param);

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

private enum canStringMarshal(T) = is(typeof(T.init.toString((in char[]) {}))) ||
        is(typeof(T.init.toString()));

// match the D type to a specific MySQL type
private template getFieldType(T)
{
    private import std.datetime;
    private import std.traits;
    alias RT = dbValueType!T;
    static if(is(RT == T))
    {
        static if(isIntegral!T)
            enum getFieldType = "INTEGER";
        else static if(canStringMarshal!T || isSomeString!T)
            enum getFieldType = "TEXT";
        else static if(isFloating!T)
            enum getFieldType = "REAL";
        else static if(isArray!T)
            enum getFieldType = "BLOB";
        else
            static assert(0, "Unknown mapping for type " ~ T.stringof);
    }
    else
        alias getFieldType = .getFieldType!RT;
}


// generate an SQL statement to insert a table definition.
template createTableSql(T, bool doForeignKeys = false)
{
    string generate()
    {
        import std.traits;
        import sqlbuilder.uda;
        import sqlbuilder.traits;
        auto result = `CREATE TABLE "` ~ getTableName!T ~ `" (`;
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
                    result ~= " AUTOINCREMENT";

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
                result ~= `"` ~ getColumnName!(kf) ~ `"`;
            }
            result ~= "),";
        }

        static if(doRelations)
        {
            import std.traits;
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

enum dropTableSql(T) = `DROP TABLE IF EXISTS "` ~ getTableName!T ~ `"`;+/
