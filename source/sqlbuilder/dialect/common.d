module sqlbuilder.dialect.common;
import sqlbuilder.traits;
import sqlbuilder.types;
import std.typecons;
import std.meta;
import std.traits;

private void append(T, R)(ref T[] arr, R stuff)
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

private void addJoin(QP...)(ref Query!QP q, const TableDef def)
{
    if(def.as in q.tables)
        // already added
        return;

    // short circuit any cycles
    q.tables[def.as] = true;

    foreach(d; def.dependencies)
        q.addJoin(d);

    if(q.joins.expr)
        q.joins.expr ~= joinSpec;
    q.joins.expr ~= def.joinExpr;
}

private void updateQuery(string field, string sep = ", ", Q, Expr...)(ref Q query, Expr expressions) if (isQuery!Q)
{
    foreach(e; expressions)
    {
        // add each table dependency to the query
        foreach(tbl; getTables(e))
            addJoin(query, tbl);
        if(__traits(getMember, query, field).expr)
            __traits(getMember, query, field).expr ~= sep;
        __traits(getMember, query, field).expr ~= e.expr;
        static if(!is(getParamType!(typeof(e)) == void))
            __traits(getMember, query, field).params.append(e.params);
    }
}

// use ref counting to handle lifetime management for now
auto select(Item = void, Cols...)(Cols cols) if (cols.length == 0 || !(isInstanceOf!(RefCounted, Cols[0]) && isQuery!(typeof(cols[0].refCountedPayload()))))
{
    // find the correct item type
    static if(is(Item == void))
        alias RealItem = getParamType!(Cols);
    else
        alias RealItem = Item;
    auto q = Query!(RealItem)().refCounted;
    return select(q, cols);
}

auto select(Q, Cols...)(RefCounted!(Q, RefCountedAutoInitialize.no) query, Cols columns) if (isQuery!Q)
{
    query.refCountedPayload.updateQuery!"fields"(columns);
    // adjust the query type list according to the columns.
    alias typeList = getQueryTypeList!(Q, Cols);
    static if(!is(typeList == Q.RowTypes))
    {
        alias QType = Query!(Q.ItemType, typeList);
        return () @trusted {
            return *cast(RefCounted!(QType, RefCountedAutoInitialize.no) *)&query; 
        }();
    }
    else
        return query;
}

auto blankQuery(Item = void)()
{
    return select!Item();
}

auto orderBy(Q, Expr...)(RefCounted!(Q, RefCountedAutoInitialize.no) query, Expr expressions) if (isQuery!Q)
{
    query.refCountedPayload.updateQuery!"orders"(expressions);
    return query;
}

auto where(Q, Spec...)(RefCounted!(Q, RefCountedAutoInitialize.no) query, Spec spec) if (isQuery!Q)
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
                query.addJoin(tab);
            static if(!is(getParamType!(typeof(s)) == void))
                query.conditions.params.append(s.params);
        }
        else
            static assert(false, "Unsupported type for where clause: " ~ typeof(s).stringof ~ ", maybe try wrapping as a parameter");
    }

    return query;
}

ColumnDef!T as(T)(ColumnDef!T col, string newName)
{
    return ColumnDef!T(col.table, col.expr ~ " AS " ~ newName.makeSpec(Spec.id));
}

// generate a query that provides complete rows of data from the server, given
// the joins between the rows. All parameters must be data sets.
auto fetch(Item = void, Sets...)(Sets sets) if (allSatisfy!(isDataSet, Sets))
{
}
