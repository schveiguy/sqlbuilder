module sqlbuilder.dialect.common;
import sqlbuilder.traits;
import sqlbuilder.types;
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

private void addJoin(Item)(ref Joins!Item join, const TableDef def)
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

private void updateQuery(string field, string sep = ", ", Q, Expr...)(ref Q query, Expr expressions) if (isQuery!Q)
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

// use ref counting to handle lifetime management for now
auto select(Item = void, Cols...)(Cols cols) if (cols.length == 0 || !isQuery!(Cols[0]))
{
    // find the correct item type
    static if(is(Item == void))
        alias RealItem = getParamType!(Cols);
    else
        alias RealItem = Item;
    return select(Query!(RealItem)(), cols);
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

auto blankQuery(Item = void)()
{
    return select!Item();
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

ColumnDef!T as(T)(ColumnDef!T col, string newName)
{
    return ColumnDef!T(col.table, col.expr ~ " AS " ~ newName.makeSpec(Spec.id));
}

Insert!Item insert(Item)(const(TableDef) table)
{
    if(table.dependencies.length)
        throw new Exception("Cannot insert into a joined table: " ~ table.as);
    return Insert!Item(table.as);
}

auto set(Item, Col, Val)(Insert!Item ins, Col column, Val value)
{
    static if(is(Col == string))
    {
        if(ins.colNames.expr)
            ins.colNames.expr ~= ", ";
        ins.colNames.expr ~= column.makeSpec(Spec.id);
    }
    else
    {
        // verify that the table definitions are identical
        foreach(tbl; getTables(column))
            if(tbl.dependencies.length || tbl.as != ins.tableid)
                throw new Exception("Adding incompatible column from a different table");

        // append to the column names
        if(ins.colNames.expr)
            ins.colNames.expr ~= ", ";
        ins.colNames.expr ~= column.expr;
        static if(!is(getParamType!(Col) == void))
            ins.colNames.params.append(col.params);
    }

    // append to the values
    if(ins.colValues.expr)
        ins.colValues.expr ~= ", ";
    ins.colValues.expr ~= paramSpec;
    import std.range : only;
    ins.colValues.params.append(only(value));
    return ins;
}

Insert!Item insert(Item, T)(T item) if (!is(T : const(TableDef)))
{
    import sqlbuilder.dataset;
    import sqlbuilder.uda;
    import std.traits;
    // figure out the table definition
    auto result = insert!Item(staticTableDef!T);

    // now, insert all the values for the columns (ignore any autoincrement items).
    foreach(fname; __traits(allMembers, T))
        static if(isField!(T, fname) &&
                  !hasUDA!(__traits(getMember, T, fname), autoIncrement))
        {
                result = result.set(getColumnName!(__traits(getMember, T, fname)), __traits(getMember, item, fname));
        }
    return result;
}

Update!Item set(Item = void, Col, Val)(Col column, Val value)
{
    static if(is(Item == void))
        Update!(getParamType!(Col, Val)) result;
    else
        Update!Item result;
    return set(result, column, value);
}

auto set(Item, Col, Val)(Update!Item upd, Col column, Val value)
{
    // add the column expression
    foreach(tbl; getTables(column))
        upd.joins.addJoin(tbl);
    if(upd.settings.expr)
        upd.settings.expr ~= ", ";
    upd.settings.expr ~= column.expr;
    static if(!is(getParamType!(Col) == void))
        upd.colNames.params.append(col.params);
    upd.settings.expr ~= " = ";
    foreach(tbl; getTables(value))
        upd.joins.addJoin(tbl);
    upd.settings.expr ~= value.expr;
    static if(!is(getParamType!(Val) == void))
        upd.settings.params.append(value.params);
    return upd;
}

// shortcut to update all the fields in a row. By default, this uses the
// primary key as the "where" clause.
Update!Item update(Item, T)(T item)
{
    import sqlbuilder.dataset;
    import sqlbuilder.uda;
    import std.traits;
    auto result = Update!Item();

    auto ds = DataSet!T.init;
    foreach(fname; __traits(allMembers, T))
    {
        static if(isField!(T, fname))
        {
            static if(hasUDA!(__traits(getMember, T, fname), primaryKey))
            {
                result = result.where(__traits(getMember, ds, fname), " = ",
                                      __traits(getMember, item, fname).param);
            }
            else
            {
                result = result.set(__traits(getMember, ds, fname),
                                    __traits(getMember, item, fname).param);
            }
        }
    }
    return result;
}

Delete!Item removeFrom(Item)(const TableDef table)
{
    if(table.dependencies.length)
        throw new Exception("Cannot delete from a joined table: " ~ table.as);
    // blank item the correct 
    Delete!Item result;
    result.joins.addJoin(table);
    return result;
}

Delete!Item remove(Item, T)(T item) if (hasPrimaryKey!T)
{
    import sqlbuilder.dataset;
    DataSet!T ds;
    return removeFrom!Item(ds.tableDef).havingKey(ds, item);
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
