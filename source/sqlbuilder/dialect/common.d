module sqlbuilder.dialect.common;
import sqlbuilder.traits;
import sqlbuilder.types;
import std.traits;

package void append(T, R)(ref T[] arr, R stuff)
{
    import std.array;
    import std.range : hasLength;
    static if(is(typeof(arr ~= stuff)))
        arr ~= stuff;
    else static if(is(typeof(arr.insertInPlace(stuff))))
    {
        arr.insertInPlace(arr.length, stuff);
    }
    else static if(is(typeof(arr[0] = stuff.front)))
    {
        static if(hasLength!R)
        {
            auto oldLen = arr.length;
            import std.algorithm : copy;
            arr.length = oldLen + stuff.length;
            auto slice = arr[oldLen .. $];
            copy(stuff, arr[oldLen .. $]);
        }
        else
        {
            foreach(item; stuff)
            {
                arr.length = arr.length + 1;
                arr[$-1] = item;
            }
        }
    }
    else
        static assert(0, "Can't append " ~ R.stringof ~ " to type " ~ T.stringof ~ "[]");
}


// wrapper to provide a mechanism to distinguish parameters from strings or
// other things.
struct Parameter(T, bool hasValidation = false)
{
    private import std.range : only;
    enum expr = paramSpec;
    alias PType = typeof(only(T.init));
    PType params;
    static if(hasValidation)
        bool valid = true;
}

auto param(T)(T item)
{
    import std.range : only;
    import std.traits : isInstanceOf;
    import std.typecons : Nullable;
    static if(isInstanceOf!(Nullable, T))
    {
        alias Val = typeof(item.get());
        if(item.isNull)
        {
            auto result = Parameter!(Val, true)(only(Val.init), false);
            result.params.popFront;
            return result;
        }
        else
            return Parameter!(Val, true)(only(item.get), true);
    }
    else
        return Parameter!T(only(item));
}

unittest 
{
    auto p = "hello".param;
    static assert(is(getParamType!(typeof(p)) == string));

    import std.typecons : Nullable, nullable;
    Nullable!int x;
    auto p2 = x.param;
    static assert(is(getParamType!(typeof(p2)) == int));
    assert(!p2.valid);
    assert(p2.params.empty);

    x = 5;
    p2 = x.param;
    assert(p2.valid);
    assert(p2.params.front == 5);
}

package void addJoin(Item)(ref Joins!Item join, const TableDef def)
{
    if(def.as in join.tables)
        // already added
        return;

    // short circuit any cycles
    join.tables[def.as] = true;

    foreach(d; def.dependencies)
        join.addJoin(d);

    if(join.expr)
        join.expr ~= joinSpec;
    join.expr ~= def.joinExpr;
}

package void updateQuery(string field, string sep = ", ", Q, Expr...)(ref Q query, Expr expressions) if (isQuery!Q)
{
    foreach(e; expressions)
    {
        // add each table dependency to the query
        foreach(tbl; getTables(e))
            query.joins.addJoin(tbl);
        if(__traits(getMember, query, field).expr)
            __traits(getMember, query, field).expr ~= sep;
        __traits(getMember, query, field).expr ~= e.expr;
        static if(!is(getParamType!(typeof(e)) == void))
            __traits(getMember, query, field).params.append(e.params);
    }
}

auto select(Q, Cols...)(Q query, Cols columns) if (isQuery!Q)
{
    query.updateQuery!"fields"(columns);
    // adjust the query type list according to the columns.
    alias typeList = getQueryTypeList!(Q, Cols);
    static if(!is(typeList == Q.RowTypes))
    {
        alias QType = Query!(Q.ItemType, typeList);
        return (() @trusted  => *cast(QType *)&query)();
    }
    else
        return query;
}

Q orderBy(Q, Expr...)(Q query, Expr expressions) if (isQuery!Q)
{
    query.updateQuery!"orders"(expressions);
    return query;
}

Q groupBy(Q, Cols...)(Q query, Cols cols) if (isQuery!Q)
{
    query.updateQuery!"groups"(cols);
    return query;
}

Q limit(Q)(Q query, size_t numItems, size_t offset = 0) if (isQuery!Q)
{
    query.limitQty = numItems;
    query.limitOffset = offset;
    return query;
}

auto where(Q, Spec...)(Q query, Spec spec) if (isQuery!Q || is(Q : Update!T, T) || is(Q : Delete!T, T))
{
    import std.array: Appender;
    import std.range : put;
    import std.traits : isSomeString, isInstanceOf;
    import std.typecons : Nullable;

    // static
    foreach(s; spec)
        static if(is(typeof(s.valid)))
            if(!s.valid)
                return query;

    if(query.conditions.expr)
        query.conditions.expr ~= " AND ";
    // static 
    foreach(s; spec)
    {
        static if(is(typeof(s) : const(char)[]))
        {
            query.conditions.expr ~= s;
        }
        else static if(is(typeof((() => s.expr)()) : const(char)[]) ||
                       is(typeof((() => s.expr)()) : const(ExprString)))
        {
            query.conditions.expr ~= s.expr;
            foreach(tab; getTables(s))
                query.joins.addJoin(tab);
            static if(!is(getParamType!(typeof(s)) == void))
                query.conditions.params.append(s.params);
        }
        else
            static assert(false, "Unsupported type for where clause: " ~ typeof(s).stringof ~ ", maybe try wrapping as a parameter");
    }

    return query;
}

auto havingKey(T, Q, Args...)(Q query, T t, Args args)
    if (isDataSet!T && hasPrimaryKey!(T.RowType) &&
          Args.length == primaryKeyFields!(T.RowType).length &&
          !is(Args[0] : T.RowType) &&
          (
             isQuery!Q ||
             is(Q : Update!X, X) ||
             is(Q : Insert!X, X) ||
             is(Q : Delete!X, X)
          )
       )
{
    foreach(i, f; primaryKeyFields!(t.RowType))
    {
        query = query.where(__traits(getMember, t, f), " = ", 
                            args[i].param);
    }
    return query;
}

auto havingKey(T, Q, U)(Q query, T t, U model)
    if (isDataSet!T && hasPrimaryKey!(T.RowType) && is(U : T.RowType) &&
          (
             isQuery!Q ||
             is(Q : Update!X, X) ||
             is(Q : Insert!X, X) ||
             is(Q : Delete!X, X)
          )
       )
{
    foreach(i, f; primaryKeyFields!(t.RowType))
    {
        query = query.where(__traits(getMember, t, f), " = ", 
                            __traits(getMember, model, f).param);
    }
    return query;
}

auto havingKey(T, Q)(Q query, T t) if (!isDataSet!T && hasPrimaryKey!T)
{
    import sqlbuilder.dataset;
    DataSet!T ds;
    return query.havingKey(ds, t);
}

auto havingKey(T, Q, Args...)(Q query, Args args)
   if (Args.length > 0 && !isDataSet!(Args[0]) && hasPrimaryKey!T)
{
    import sqlbuilder.dataset;
    DataSet!T ds;
    return query.havingKey(ds, args);
}

ColumnDef!T as(T)(ColumnDef!T col, string newName)
{
    return ColumnDef!T(col.table, col.expr ~ " AS " ~ newName.makeSpec(Spec.id));
}
