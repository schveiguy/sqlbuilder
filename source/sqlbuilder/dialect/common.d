module sqlbuilder.dialect.common;
import sqlbuilder.traits;
import sqlbuilder.types;
import std.traits;
import std.range : empty, popFront, front;


// catch-all for things that don't define a dbValue conversion. We also strip
// any enum types from the value.
/*package(sqlbuilder) auto ref dbValue(T)(auto ref T item)
{
    static if(is(T == enum))
    {
        import std.traits : OriginalType;
        return cast(OriginalType!T)item;
    }
    else
    {
        pragma(inline, true);
        return item;
    }
}*/

package void append(T, R)(ref T[] arr, R stuff)
{
    import std.range : hasLength;
    static if(is(typeof(arr ~= stuff)))
    {
        arr ~= stuff;
    }
    else static if(is(typeof(arr[0] = stuff.front)))
    {
        static if(hasLength!R)
        {
            auto oldLen = arr.length;
            import std.algorithm : copy;
            arr.length = oldLen + stuff.length;
            copy(stuff, arr[oldLen .. $]);
        }
        else
        {
            foreach(item; stuff)
            {
                static if(is(typeof(arr ~= item)))
                    arr ~= item;
                else
                {
                    // maybe only works with assignment
                    arr.length = arr.length + 1;
                    arr[$-1] = item;
                }
            }
        }
    }
    /*static if(is(typeof(arr[0] = stuff.front.dbValue)))
    {
        static if(hasLength!R)
        {
            auto oldLen = arr.length;
            import std.algorithm : copy, map;
            arr.length = oldLen + stuff.length;
            auto slice = arr[oldLen .. $];
            copy(stuff.map!(v => v.dbValue), arr[oldLen .. $]);
        }
        else
        {
            foreach(item; stuff)
            {
                static if(is(typeof(arr ~= item.dbValue)))
                    arr ~= item.dbValue;
                else
                {
                    // maybe only works with assignment
                    arr.length = arr.length + 1;
                    arr[$-1] = item.dbValue;
                }
            }
        }
    }*/
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

package void addJoin(Item)(ref Joins!Item join, const TableDef def)
{
    if(def.as in join.tables)
        // already added
        return;

    // short circuit any cycles
    join.tables[def.as] = true;

    foreach(d; def.dependencies)
        join.addJoin(d);

    join.expr ~= def.joinExpr;
}

package void updateQuery(string field, bool allowDatasets, Q, Expr...)(ref Q query, Expr expressions) if (isQuery!Q)
{
    foreach(exp; expressions)
    {
        // convert dataset expressions into the allColumns member (this works
        // only for selects)
        static if(allowDatasets && isDataSet!(typeof(exp)))
            auto e = exp.allColumns;
        else
            alias e = exp;
        // add each table dependency to the query
        foreach(tbl; getTables(e))
            query.joins.addJoin(tbl);
        if(__traits(getMember, query, field).expr)
            __traits(getMember, query, field).expr ~= ", ";
        __traits(getMember, query, field).expr ~= e.expr;
        static if(!is(getParamType!(typeof(e)) == void))
            __traits(getMember, query, field).params.append(e.params);
    }
}

auto select(Q, Cols...)(Q query, Cols columns) if (isQuery!Q)
{
    query.updateQuery!("fields", true)(columns);
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
    query.updateQuery!("orders", false)(expressions);
    return query;
}

Q groupBy(Q, Cols...)(Q query, Cols cols) if (isQuery!Q)
{
    query.updateQuery!("groups", false)(cols);
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

    // static
    foreach(s; spec)
        static if(is(typeof(s.valid)))
            if(!s.valid)
                return query;

    if(query.conditions.expr)
        query.conditions.expr ~= andSpec;
    // static 
    foreach(i, s; spec)
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
        {
            enum int pnum = i + 1;
            static assert(false, "Unsupported type for where clause: " ~ typeof(s).stringof ~ " (arg " ~ pnum.stringof ~ "), maybe try wrapping with `sqlbuilder.dialect.common.param`");
        }
    }

    return query;
}

ColumnDef!T as(T)(ColumnDef!T col, string newName)
{
    return ColumnDef!T(col.table, col.expr ~ " AS " ~ newName.makeSpec(Spec.id));
}

ColumnDef!T ascend(T)(ColumnDef!T col)
{
    return ColumnDef!T(col.table, col.expr ~ " ASC");
}

ColumnDef!T descend(T)(ColumnDef!T col)
{
    return ColumnDef!T(col.table, col.expr ~ " DESC");
}

// template to implement all functions that require a specific parameter type.
// Making this a template means we can swap out the type that is used as the
// liason between the database library and our library.
template SQLImpl(Item, alias param)
{

    // use ref counting to handle lifetime management for now
    auto select(Cols...)(Cols cols) if (cols.length == 0 || !isQuery!(Cols[0]))
    {
        return select(Query!Item(), cols);
    }

    alias select = sqlbuilder.dialect.common.select;

    Insert!Item insert(const(TableDef) table)
    {
        if(table.dependencies.length)
            throw new Exception("Cannot insert into a joined table: " ~ table.as);
        return Insert!Item(table.as);
    }

    Insert!Item set(Col, Val)(Insert!Item ins, Col column, Val value)
    {
        if(!getTables(value).empty)
            throw new Exception("Table dependencies are not allowed for inserting rows");
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
        ins.colValues.expr ~= value.expr;
        static if(!is(getParamType!(Val) == void))
            ins.colValues.params.append(value.params);
        return ins;
    }

    Insert!Item insert(T)(T item) if (!is(T : const(TableDef)))
    {
        import sqlbuilder.dataset;
        import sqlbuilder.uda;
        import std.traits;
        // figure out the table definition
        auto result = insert(staticTableDef!T);

        // now, insert all the values for the columns (ignore any autoincrement items).
        foreach(fname; __traits(allMembers, T))
            static if(isField!(T, fname) &&
                      !hasUDA!(__traits(getMember, T, fname), autoIncrement))
            {
                result = result.set(getColumnName!(__traits(getMember, T, fname)),
                                    param(__traits(getMember, item, fname)));
            }
        return result;
    }

    Update!Item set(Col, Val)(Col column, Val value)
    {
        Update!Item result;
        return set(result, column, value);
    }

    Update!Item set(Col, Val)(Update!Item upd, Col column, Val value)
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
    Update!Item update(T)(T item)
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
                                          param(__traits(getMember, item, fname)));
                }
                else
                {
                    result = result.set(__traits(getMember, ds, fname),
                                        param(__traits(getMember, item, fname)));
                }
            }
        }
        return result;
    }

    Delete!Item removeFrom(const TableDef table)
    {
        if(table.dependencies.length)
            throw new Exception("Cannot delete from a joined table: " ~ table.as);
        Delete!Item result;
        result.joins.addJoin(table);
        return result;
    }

    Delete!Item remove(T)(T item) if (hasPrimaryKey!T)
    {
        import sqlbuilder.dataset;
        DataSet!T ds;
        return removeFrom(ds.tableDef).havingKey(ds, item);
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
                                param(__traits(getMember, model, f)));
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
                                param(args[i]));
        }
        return query;
    }
}
