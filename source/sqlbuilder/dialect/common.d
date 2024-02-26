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
    if(join.hasJoin(def))
        // already added
        return;

    // short circuit any cycles
    // TODO: see how we can possibly do this
    //join.tables[def.as] = true;

    if(def.dependencies.length == 0)
    {
        // this is the primary table. Only add it if there are no other joins
        if(join.expr.data.length != 0)
            throw new Exception("Multiple primary tables not allowed");
    }
    else foreach(d; def.dependencies)
        join.addJoin(d);

    join.expr ~= def.joinExpr;
}

package void updateQueryField(bool allowDatasets, Item, Expr...)(ref SQLFragment!Item field, ref Joins!Item joins, Expr expressions)
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
            joins.addJoin(tbl);
        if(field.expr)
            field.expr ~= ", ";
        field.expr ~= e.expr;
        static if(!is(getParamType!(typeof(e)) == void))
            field.params.append(e.params);
    }
}

auto select(Q, Cols...)(Q query, Cols columns) if (isQuery!Q)
{
    updateQueryField!true(query.fields, query.joins, columns);
    // adjust the query type list according to the columns.
    alias typeList = getQueryTypeList!(Q, Cols);
    static if(!is(typeList == Q.RowTypes))
    {
        alias QType = Query!(Q.ItemType, typeList);
        return QType(query.tupleof);
    }
    else
        return query;
}

auto changed(T)(ColumnDef!T col)
{
    // create a column def based on the given column
    return ColumnDef!(Changed!T)(col.table, col.expr);
}

auto changed(DS)(DS ds) if (isDataSet!DS && hasPrimaryKey!(DS.RowType))
{
    // create a column change def based on the dataset's primary keys
    ExprString expr;
    foreach(i, f; primaryKeyFields!(DS.RowType))
    {
        static if(i > 0)
            expr ~= ", ";
        expr ~= ds.tableDef.as.makeSpec(Spec.tableid);
        expr ~= getColumnName!(__traits(getMember, DS.RowType, f));
    }
    return ColumnDef!(Changed!(PrimaryKeyTypes!(DS.RowType)))(ds.tableDef, expr);
}

Q orderBy(Q, Expr...)(Q query, Expr expressions) if (isQuery!Q)
{
    updateQueryField!false(query.orders, query.joins, expressions);
    return query;
}

Q groupBy(Q, Cols...)(Q query, Cols cols) if (isQuery!Q)
{
    updateQueryField!false(query.groups, query.joins, cols);
    return query;
}

Q limit(Q)(Q query, size_t numItems, size_t offset = 0) if (isQuery!Q)
{
    query.limitQty = numItems;
    query.limitOffset = offset;
    return query;
}

enum ConditionalJoiner
{
    none,
    and,
    or,
}

void updateConditions(Item, Spec...)(ref SQLFragment!Item conditions, ref Joins!Item joins, Spec spec) if (Spec.length > 0)
{
    // static
    foreach(s; spec)
        static if(is(typeof(s.valid)))
            if(!s.valid)
                return;

    static if(is(Spec[0] == string))
    {
        if(spec[0] != endGroupSpec)
            conditions.expr.addSep;
    }
    else
        conditions.expr.addSep;
    // static
    foreach(i, s; spec)
    {
        static if(is(typeof(s) : const(char)[]))
        {
            conditions.expr ~= s;
        }
        else static if(is(typeof((() => s.expr)()) : const(char)[]) ||
                       is(typeof((() => s.expr)()) : const(ExprString)))
        {
            conditions.expr ~= s.expr;
            foreach(tab; getTables(s))
                joins.addJoin(tab);
            static if(!is(getParamType!(typeof(s)) == void))
                conditions.params.append(s.params);
        }
        else
        {
            enum int pnum = i + 1;
            static assert(false, "Unsupported type for where clause: " ~ typeof(s).stringof ~ " (arg " ~ pnum.stringof ~ "), maybe try wrapping with `param`");
        }
    }
}

Q where(Q, Spec...)(Q query, Spec spec) if ((isQuery!Q || is(Q : Update!T, T) || is(Q : Delete!T, T)) && Spec.length > 0)
{
    updateConditions(query.conditions, query.joins, spec);
    return query;
}
// This function simplifies the conditional expression string based on the
// grouping tokens. This will eliminate empty groupings (and the separators
// surrounding it), and also remove extraneous groupings.
//
// If the term "NOT" (in any captialization/spacing) is detected before the
// grouping, it is considered part of the grouping, and also removed.
//
// It also uses boolean logic to remove extraneous conditions (like FALSE AND
// someCondition will remove someCondition). It will NOT remove joins that are
// no longer needed, due to the condition being removed, so you still must deal
// with possible group by clauses.
//
// This function rewrites the expression array and possibly the parameter
// array, which means that you should only use this function when you know
// there is only one reference to this data.
//
// it accepts the aggregate by reference, and returns a reference to it at the
// end.
//
ref Q simplifyConditions(Q)(return ref Q query) if (isQuery!Q || is(Q : Update!T, T) || is(Q : Delete!T, T))
{
    import std.exception : enforce;

    enum hasParams = is(typeof(query.conditions.params));

    enum TermType
    {
        not,
        group,
        bTrue,
        bFalse,
        endGroup,
        endData,
        other
    }

    static struct WherePrinter
    {
        string[] data;
        void toString(Out)(Out outputRange)
        {
            import std.range : put;
            import std.format : formattedWrite;
            import sqlbuilder.util;
            put(outputRange, "`");
            BitStack andor;
            andor.push(true);
            
            foreach(d; data)
            {
                with(Spec) final switch(getSpec(d))
                {
                case none:
                    put(outputRange, d);
                    break;
                case id:
                    put(outputRange, "`");
                    put(outputRange, d[2 .. $]);
                    put(outputRange, "`");
                    break;
                case tableid:
                    put(outputRange, "`");
                    put(outputRange, d[2 .. $]);
                    put(outputRange, "`.");
                    break;
                case param:
                    if(d.length == 2)
                        put(outputRange, " ? ");
                    else
                        put(outputRange, " # ");
                    break;
                case leftJoin:
                case rightJoin:
                case innerJoin:
                case outerJoin:
                case objend:
                    formattedWrite(outputRange, "%s", getSpec(d));
                    break;
                case separator:
                    put(outputRange, andor.peek ? " && " : " || ");
                    break;
                case beginAnd:
                    andor.push(true);
                    put(outputRange, "(");
                    break;
                case beginOr:
                    andor.push(false);
                    put(outputRange, "(");
                    break;
                case endGroup:
                    if(!andor.length)
                        throw new Exception("invalid nesting");
                    andor.pop;
                    put(outputRange, ")");
                    break;
                }
            }
            put(outputRange, "`");
        }
        string toString()
        {
            import std.array;
            Appender!string app;
            toString(app);
            return app.data;
        }
    }

    //import std.stdio;
    //writeln("About to simplify: ", WherePrinter(query.conditions.expr.data));

    static struct TermInfo
    {
        size_t bidx;
        size_t eidx;
        size_t subterms;
        bool hasNot;
        TermType type;
    }

    static struct Simplifier
    {
        string[] data;
        static TermType parseTerm(string s)
        {
            import std.string : strip, toUpper;
            import std.algorithm : equal;
            s = s.strip;
            if(s.length)
            {
                switch(s[0])
                {
                case 'T': case 't':
                    if(s.length == 4 && equal(s[1 .. $].toUpper, "RUE"))
                        return TermType.bTrue;
                    break;
                case 'F': case 'f':
                    if(s.length == 5 && equal(s[1 .. $].toUpper, "ALSE"))
                        return TermType.bFalse;
                    break;
                case 'N': case 'n':
                    if(s.length == 3 && equal(s[1 .. $].toUpper, "OT"))
                        return TermType.not;
                    break;
                default:
                    break;
                }
            }
            return TermType.other;
        }

        size_t nextSignificant(size_t eidx)
        {
            while(eidx < data.length)
            {
                with(Spec) switch(getSpec(data[eidx]))
                {
                case separator:
                    break;
                case none:
                    if(data[eidx].length == 0)
                        break;
                    return eidx;
                    static if(hasParams)
                    {
                    case param:
                        if(data[eidx].length == 2) // skip removed params
                            return eidx;
                        break;
                    }
                default:
                    return eidx;
                }
                ++eidx;
            }
            return eidx;
        }

        size_t nextSeparator(size_t eidx)
        {
            while(eidx < data.length)
            {
                with(Spec) switch(getSpec(data[eidx]))
                {
                case endGroup:
                case separator:
                    return eidx;
                default:
                    break;
                }
                ++eidx;
            }
            return eidx;
        }

        // skip to the end of the group. This is ONLY valid for sub groups,
        // not the outer group.
        size_t skipToGroupEnd(size_t idx)
        {
            size_t nesting = 1;
            while(++idx < data.length)
            {
                with(Spec) switch(getSpec(data[idx]))
                {
                case beginAnd:
                case beginOr:
                    ++nesting;
                    break;
                case endGroup:
                    if(--nesting == 0)
                        return idx;
                    break;
                default:
                    break;
                }
            }
            if(nesting != 1)
                throw new Exception("Invalid group construction");
            return data.length - 1;
        }

        enum removedParam = paramSpec ~ "X";

        void clearData(size_t bidx, size_t eidx)
        {
            static if(hasParams)
            {
                foreach(ref d; data[bidx .. eidx])
                {
                    if(getSpec(d) == Spec.param)
                        d = removedParam;
                    else
                        d = null;
                }
            }
            else
            {
                data[bidx .. eidx] = null;
            }
        }

        TermInfo nextTerm(size_t eidx)
        {
            // skip all blanks and separators
            TermInfo result;
            result.bidx = eidx = nextSignificant(eidx);
            if(result.bidx == data.length)
            {
                result.eidx = result.bidx;
                result.type = TermType.endData;
                return result;
            }
            auto sp = getSpec(data[eidx]);
            with(Spec) switch(sp)
            {
            case beginAnd:
            case beginOr:
                result = processSubgroup(sp, eidx + 1);
                //writeln("processed a subgroup: ", result);
                if(result.type == TermType.group && result.subterms == 0)
                {
                    // empty group, remove it completely
                    //writeln("empty group: ", WherePrinter(data[result.bidx .. result.eidx + 1]));
                    clearData(result.bidx, result.eidx + 1);
                    // recurse, just find the next term.
                    return nextTerm(result.eidx + 1);
                }
                return result;
            case endGroup:
                result.type = TermType.endGroup;
                result.eidx = eidx;
                return result;
            default:
                // check for specialized cases
                {

                    auto termType = parseTerm(data[result.bidx]);
                    if(termType == TermType.not)
                    {
                        TermInfo modified = nextTerm(result.bidx + 1);
                        if(modified.hasNot)
                        {
                            // not not turns into just the thing. The inner
                            // not is located at the first index. Cancel
                            // both of them.
                            data[modified.bidx] = "";
                            data[result.bidx] = "";
                            result.bidx = nextSignificant(modified.bidx + 1);
                        }
                        else if(modified.type == TermType.bFalse)
                        {
                            // "NOT FALSE" is really just "TRUE"
                            modified.type = TermType.bTrue;
                            data[result.bidx] = " TRUE ";
                            data[modified.bidx] = "";
                        }
                        else if(modified.type == TermType.bTrue)
                        {
                            // "NOT TRUE" is really just "FALSE"
                            modified.type = TermType.bFalse;
                            data[result.bidx] = " FALSE ";
                            data[modified.bidx] = "";
                        }
                        else if(modified.type == TermType.endGroup ||
                                modified.type == TermType.endData)
                        {
                            // this is a stray not, remove it.
                            data[result.bidx] = "";
                            return modified;
                        }
                        else
                        {
                            // apply the 'not' to it.
                            result.hasNot = true;
                        }
                        result.eidx = modified.eidx;
                        result.type = modified.type;
                        result.subterms = modified.subterms;
                        return result;
                    }

                    // find the next punctuation item
                    result.eidx = nextSeparator(result.bidx) - 1;
                    result.type = termType;
                    return result;
                }
            }
        }

        TermInfo processSubgroup(Spec myGroup, size_t eidx)
        {
            TermInfo firstItem;
            TermInfo result;
            import std.stdio;
            import std.conv;
            result.bidx = eidx == 0 ? eidx : eidx - 1;
            /+immutable firstidx = result.bidx;
            immutable endidx = skipToGroupEnd(firstidx) + 1;
            string beforeData = text(WherePrinter(data[firstidx .. endidx]));
            scope(exit)
            {
                writeln(myGroup, ": before group was ", beforeData, "\nafter is ", WherePrinter(data[firstidx .. endidx]));
                writeln("about to return ", result);
            }+/
loop:
            while(true)
            {
                auto item = nextTerm(eidx);
                ++result.subterms;
                if(firstItem.type == TermType.not)
                {
                    firstItem = item;
                }

                with(TermType) final switch(item.type)
                {
                case group:
                    // this is a subgroup term. If it's type matches our
                    // type (and no not is applied), then we can just
                    // remove it's walls, and include it in our terms.
                    //writeln("got group of ", item);
                    if(!item.hasNot && getSpec(data[item.bidx]) == myGroup)
                    {
                        data[item.bidx] = "";
                        data[item.eidx] = "";
                        result.subterms += item.subterms - 1;
                    }
                    break;
                case bTrue:
                    if(myGroup == Spec.beginAnd)
                    {
                        // eliminate if not the first term
                        if(result.subterms > 1)
                        {
                            data[item.bidx] = "";
                            --result.subterms;
                        }
                    }
                    else if(myGroup == Spec.beginOr)
                    {
                        // this entire group reduces down to just TRUE.
                        result.eidx = skipToGroupEnd(item.eidx);
                        //writeln("reducing to true: ", WherePrinter(data[item.bidx .. item.eidx + 1]));
                        data[result.bidx] = data[item.bidx];
                        clearData(result.bidx + 1, result.eidx + 1);
                        result.type = item.type;
                        result.subterms = 0;
                        return result;
                    }
                    break;
                case bFalse:
                    if(myGroup == Spec.beginOr)
                    {
                        // eliminate if not the first term
                        if(result.subterms > 1)
                        {
                            data[item.bidx] = "";
                            --result.subterms;
                        }
                    }
                    else if(myGroup == Spec.beginAnd)
                    {
                        // this entire group reduces down to just FALSE.
                        result.eidx = skipToGroupEnd(item.eidx);
                        //writeln("reducing to false: ", WherePrinter(data[item.bidx .. item.eidx + 1]));
                        data[result.bidx] = data[item.bidx];
                        clearData(result.bidx + 1, result.eidx + 1);
                        result.type = item.type;
                        result.subterms = 0;
                        return result;
                    }
                    break;
                case other:
                    // no special treatment, just another term.
                    break;
                case endData:
                    if(result.bidx != 0)
                        throw new Exception("Invalid group structure, reached end of data");
                    goto case;
                case endGroup:
                    --result.subterms;
                    result.eidx = item.eidx;
                    break loop;
                case not:
                    assert(0); // should never get here
                }
                // remove the first item if it's extraneous (true in an and
                // group, or false in an or group)
                if(result.subterms > 1 &&
                   ((myGroup == Spec.beginAnd && firstItem.type == TermType.bTrue) ||
                    (myGroup == Spec.beginOr && firstItem.type == TermType.bFalse)))
                {
                    data[firstItem.bidx] = "";
                    firstItem = item;
                    --result.subterms;
                }

                // go to next term.
                eidx = item.eidx + 1;
            }

            // check to see if this is a single item group. If so, we can
            // eliminate the group, and return the information for the
            // first item.
            if(result.subterms == 1)
            {
                // do not blank out anything that the first item considers part
                // of it. This can happen for the outer group (that has no
                // beginAnd)
                if(result.bidx != firstItem.bidx)
                    data[result.bidx] = "";
                if(result.eidx < data.length && result.eidx != firstItem.eidx)
                    data[result.eidx] = "";
                return result = firstItem;
            }

            // it must be a group at this point.
            result.type = TermType.group;
            return result;
        }
    }

    auto data = query.conditions.expr.data;
    // ensure this isn't a slice of something.
    if(data.capacity == 0)
        data = data.dup;
    auto s = Simplifier(data);
    auto result = s.processSubgroup(Spec.beginAnd, 0);
    /*import std.stdio;
    writeln("before cleanup: ", query.conditions);
    scope(exit) writeln("after cleanup: ", query.conditions);*/

    static if(hasParams)
    {
        // ensure this isn't a slice of something.
        if(query.conditions.params.capacity == 0)
            query.conditions.params = query.conditions.params.dup;
    }

    if(result.type == TermType.bTrue)
    {
        // no reason to spit out WHERE TRUE
        query.conditions.expr.data.length = 0;
        query.conditions.expr.data.assumeSafeAppend;
        static if(hasParams)
        {
            query.conditions.params.length = 0;
            query.conditions.params.assumeSafeAppend;
        }
        return query;
    }

    // remove all extra separators, empty strings, and unneeded parameters.
    size_t widx;
    static if(hasParams)
    {
        size_t rpidx;
        size_t wpidx;
    }
    bool outputsep = false;
    bool firstItem = true;
    foreach(ridx; 0 .. data.length)
    {
        if(data[ridx].length)
        {
            bool copy = true;
            with(Spec) switch(getSpec(data[ridx]))
            {
            case separator:
                copy = false;
                if(!firstItem)
                    outputsep = true;
                break;
            case endGroup:
                firstItem = false;
                outputsep = false;
                break;
            case beginAnd:
            case beginOr:
                firstItem = true;
                break;
                static if(hasParams)
                {
                case param:
                    if(data[ridx].length == 2)
                    {
                        // legit parameter
                        query.conditions.params[wpidx++] = query.conditions.params[rpidx++];
                        firstItem = false;
                    }
                    else
                    {
                        // removed parameter
                        ++rpidx;
                        copy = false;
                    }
                    break;
                }
            default:
                firstItem = false;
                break;
            }
            if(copy)
            {
                if(outputsep)
                {
                    outputsep = false;
                    data[widx++] = sepSpec;
                }
                data[widx++] = data[ridx];
            }
        }
    }
    if(widx != data.length)
    {
        query.conditions.expr.data = data[0 .. widx];
        query.conditions.expr.data.assumeSafeAppend;
    }
    static if(hasParams)
    {
        if(wpidx != rpidx)
        {
            query.conditions.params = query.conditions.params[0 .. wpidx];
            query.conditions.params.assumeSafeAppend;
        }
    }
    return query;
}

unittest
{
    import sqlbuilder.dataset;
    static struct testDB
    {
        int x;
        int y;
        int z;
    }

    DataSet!testDB ds;

    static auto mkexpr(Args...)(Args args)
    {
        ExprString result;
        foreach(arg; args)
        {
            static if(is(typeof(arg) == string))
                result ~= arg;
            else
                result ~= arg.expr;
        }
        return result;
    }

    // start with no parameters
    Query!(void) uq;
    {
        auto query = uq.select(ds)
            .where(andSpec, ds.x, " = 5").where(ds.y, " = 6")
            .where("NOT ", orSpec, andSpec, endGroupSpec, endGroupSpec, endGroupSpec);
        assert(query.simplifyConditions.conditions.expr ==
               mkexpr(ds.x, " = 5", sepSpec, ds.y, " = 6"));
    }
    {
        auto query = uq.select(ds)
            .where(orSpec, ds.x, " = 5", endGroupSpec);
        assert(query.simplifyConditions.conditions.expr ==
               mkexpr(ds.x, " = 5"));
    }
    {

        auto query = uq.select(ds)
            .where(andSpec, ds.x, " = 5")
            .where(orSpec, ds.y, " = 6", endGroupSpec, endGroupSpec);
        assert(query.simplifyConditions.conditions.expr ==
               mkexpr(ds.x, " = 5", sepSpec, ds.y, " = 6"));
    }
    {
        auto query = uq.select(ds)
            .where(andSpec, ds.x, " = 5")
            .where("NOT ", orSpec, ds.y, " = 6", endGroupSpec, endGroupSpec);
        assert(query.simplifyConditions.conditions.expr ==
               mkexpr(ds.x, " = 5", sepSpec, "NOT ", ds.y, " = 6"));
    }
    {
        auto query = uq.select(ds)
            .where(andSpec, ds.x, " = 5")
            .where(andSpec, ds.y, " = 6")
            .where(ds.z, " = 7", endGroupSpec, endGroupSpec);
        assert(query.simplifyConditions.conditions.expr ==
               mkexpr(ds.x, " = 5", sepSpec, ds.y, " = 6", sepSpec, ds.z, " = 7"));
    }
    {
        auto query = uq.select(ds)
            .where(ds.x, " = 5")
            .where(orSpec, andSpec, ds.y, " = 6")
            .where(ds.z, " = 7", endGroupSpec, endGroupSpec);
        assert(query.simplifyConditions.conditions.expr ==
               mkexpr(ds.x, " = 5", sepSpec, ds.y, " = 6", sepSpec, ds.z, " = 7"));
    }
    {
        auto query = uq.select(ds)
            .where(ds.x, " = 5")
            .where(orSpec, andSpec, endGroupSpec)
            .where(andSpec, ds.y, " = 6")
            .where(ds.z, " = 7", endGroupSpec, endGroupSpec);
        assert(query.simplifyConditions.conditions.expr ==
               mkexpr(ds.x, " = 5", sepSpec, ds.y, " = 6", sepSpec, ds.z, " = 7"));
    }

    // test true/false folding
    {
        auto query = uq.select(ds)
            .where(" TRUE ").where(" TRUE ").where(" TRUE ");
        assert(query.simplifyConditions.conditions.expr.data.length == 0);
    }
    {
        auto query = uq.select(ds)
            .where(orSpec, " FALSE ")
            .where(" FALSE ")
            .where(" FALSE ", endGroupSpec);
        assert(query.simplifyConditions.conditions.expr ==
               ExprString(" FALSE "));
    }
    {
        auto query = uq.select(ds)
            .where(orSpec, ds.x, " = 5")
            .where("TRUE", endGroupSpec);
        assert(query.simplifyConditions.conditions.expr.data.length == 0);
    }
    {
        auto query = uq.select(ds).where(andSpec, " FALSE ", endGroupSpec);
        assert(query.simplifyConditions.conditions.expr ==
               ExprString(" FALSE "));
    }
    {
        auto query = uq.select(ds)
            .where(ds.x, " = 5")
            .where(" FALSE ");
        assert(query.simplifyConditions.conditions.expr ==
               ExprString(" FALSE "));
    }

    // test issue with removing front of global orSpec data.
    {
        auto query = uq.select(ds)
            .where(orSpec, orSpec, ds.x, " = 5")
            .where(ds.x, " = 6", endGroupSpec)
            .where(orSpec, " FALSE ", endGroupSpec, endGroupSpec);

        assert(query.simplifyConditions.conditions.expr ==
               mkexpr(orSpec, ds.x, " = 5", sepSpec, ds.x, " = 6", endGroupSpec));
    }

    // test removing some parameters

    import std.variant;
    Query!Variant vq;
    static auto mkparam(T)(T val)
    {
        import std.range : only;
        return Parameter!Variant(only(Variant(val)));
    }

    {
        auto query = vq.select(ds)
            .where(ds.x, " = ", mkparam(5))
            .where(orSpec, ds.y, " = ", mkparam(6))
            .where("TRUE", endGroupSpec)
            .where(ds.z, " = ", mkparam(7));
        query.simplifyConditions;
        assert(query.conditions.expr ==
               mkexpr(ds.x, " = ", paramSpec, sepSpec, ds.z, " = ", paramSpec));
        assert(query.conditions.params ==
               [Variant(5), Variant(7)]);
    }

    // test for nested not bug where subterms wasn't forwarded
    {
        auto query = uq.select(ds)
            .where(orSpec, " NOT ", orSpec, ds.x, " = 5", sepSpec, ds.y, " = 5", endGroupSpec, endGroupSpec);
        query.simplifyConditions;
        assert(query.conditions.expr ==
               mkexpr(" NOT ", orSpec, ds.x, " = 5", sepSpec, ds.y, " = 5", endGroupSpec));
    }
    {
        auto query = uq.select(ds)
            .where(" NOT ", orSpec, endGroupSpec);
        query.simplifyConditions;
        assert(query.conditions.expr == ExprString());
    }
}

ColumnDef!T exprCol(T, Args...)(Args args)
{
    // first, find all columns, and ensure that table defs are all from the
    // same table (a ColumnDef cannot have multiple tables).
    const(TableDef)* tabledef;
    foreach(ref arg; args)
    {
        static if(is(typeof(arg) == ColumnDef!U, U))
        {
            if(tabledef && arg.table != *tabledef)
                throw new Exception("can't have multiple tabledefs in the expression");
            else
                tabledef = &arg.table;
        }
    }

    assert(tabledef !is null);

    // build the expr string
    ExprString expr;
    foreach(ref a; args)
    {
        static if(is(typeof(a) == string))
            expr ~= a;
        else
            expr ~= a.expr;
    }
    return ColumnDef!(T)(*tabledef, expr);
}

ColumnDef!T as(T)(ColumnDef!T col, string newName)
{
    return exprCol!T(col, " AS ", newName.makeSpec(Spec.id));
}

T withoutAs(T)(T col)
{
    // remove any AS designation.
    with(col.expr)
        if(data.length > 2 && data[$-2] == " AS ")
            data = data[0 .. $-2];
    return col;
}

ConcatDef as(ConcatDef col, string newName)
{
    return ConcatDef(col.tables, col.expr ~ " AS " ~ newName.makeSpec(Spec.id));
}

ColumnDef!long count(T)(ColumnDef!T col)
{
    return exprCol!long("COUNT(", col, ")");
}

C ascend(C)(C col)
{
    col.expr = col.expr ~ " ASC";
    return col;
}

C descend(C)(C col)
{
    col.expr = col.expr ~ " DESC";
    return col;
}

ConcatDef concat(Args...)(Args args) if (Args.length > 1)
{
    // build an ExprString based on the args, concatenating all the tables
    // referenced.
    ConcatDef result;
    result.expr ~= "CONCAT(";
    foreach(i, arg; args)
    {
        foreach(tbl; arg.getTables)
            result.tables ~= tbl;
        static if(i != 0)
            result.expr ~= ", ";
        static if(is(typeof(arg) == string))
            result.expr ~= arg;
        else
            result.expr ~= arg.expr;
    }
    result.expr ~= ")";
    return result;
}

// template to implement all functions that require a specific parameter type.
// Making this a template means we can swap out the type that is used as the
// liason between the database library and our library.
//
// NOTE: omitting the table id for update is a requirement for sqlite, and this
// is somewhat of a hack but I can't think of a better way to do this.
template SQLImpl(Item, alias param, bool noTableIdForUpdate = false)
{

    // use ref counting to handle lifetime management for now
    auto select(Cols...)(Cols cols) if (cols.length == 0 || !isQuery!(Cols[0]))
    {
        return select(Query!Item(), cols);
    }

    alias select = sqlbuilder.dialect.common.select;

    auto /* Insert!Item */ insert(const(TableDef) table)
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
        static if(noTableIdForUpdate)
        {
            import std.algorithm : filter;
            upd.settings.expr.data.append(column.expr.data.filter!(ex => getSpec(ex) != Spec.tableid));
        }
        else
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
                    updateConditions(result.conditions, result.joins, __traits(getMember, ds, fname), " = ",
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

    auto /* Update!Item */ update()
    {
        return Update!Item.init;
    }


    auto /* Delete!Item */ removeFrom(const TableDef table)
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
            updateConditions(query.conditions, query.joins,
                 __traits(getMember, t, f), " = ", param(__traits(getMember, model, f)));
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
            updateConditions(query.conditions, query.joins,
                 __traits(getMember, t, f), " = ", param(args[i]));
        }
        return query;
    }
}
