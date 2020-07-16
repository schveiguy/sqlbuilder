module sqlbuilder.dialect.impl;
import sqlbuilder.dialect.common;
import sqlbuilder.types;
import sqlbuilder.traits;

// template to implement all functions that require a specific parameter type.
// Making this a template means we can swap out the type that is used as the
// liason between the database library and our library.
template SQLImpl(Item)
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
                                    __traits(getMember, item, fname));
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

    Delete!Item removeFrom(const TableDef table)
    {
        if(table.dependencies.length)
            throw new Exception("Cannot delete from a joined table: " ~ table.as);
        // blank item the correct 
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

}
