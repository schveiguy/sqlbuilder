/**
 * The common dialect for sql combines all the common pieces of SQL statements,
 * which are then used in specific dialects to build statements. It is expected
 * to import a specific dialect, which will use the common dialect to build
 * statements. All specific dialects use ONLY symbols from this file to build
 * statements, but use a specific Item type with the SQLImpl template.
 *
 * The SQLImpl tempalte serves as the base namespace for specific dialects.
 * Each dielect uses the common module + the SQLImpl instance which maps with
 * the base SQL data type for that module. Typically, an SQL library has a
 * basic type (either std.variant.Variant or otherwise) which is used to store
 * data for sending to and receiving from the database.
 *
 * Most functions accept and return statements *by value*. This allows building
 * base statements and then amending them for specific purposes. At the moment,
 * sqlbuilder relies on GC allocations and appending to ensure this works
 * reasonably well.
 *
 * Copyright: 2021 Steven Schveighoffer
 * License: Boost-1.0, see LICENSE.md
 */
module sqlbuilder.dialect.common;
import sqlbuilder.traits;
import sqlbuilder.types;
import std.traits;
import std.range : empty, popFront, front;


// helper utility to append to an array any kind of range of things.
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
    else
        static assert(0, "Can't append " ~ R.stringof ~ " to type " ~ T.stringof ~ "[]");
}


/**
 * This encapsulates a parameter for using in `where`, or `set` functions. A
 * parameter is passed via prepared statements, and not subject to possible SQL
 * injection attacks. Use the dialect-specific `param` or `optional` functions.
 *
 * if `hasValidation` is true, then a separate runtime boolean is used to let
 * any condition update ignore the clause. This is a common use case where you
 * may want to optionally include a clause based on whether a parameter is
 * supplied (imagine a web filter where you add filter clauses if a parameter is
 * included). Because there aren't any current conventions for this, it is left
 * up to the application when to allow this. Use the `optional` function to
 * enable this feature.
 */
struct Parameter(T, bool hasValidation = false)
{
    private import std.range : only;
    /// All Parameters translate to a parameter in SQL
    enum expr = paramSpec;
    /// The parameter list (a range of one item)
    alias PType = typeof(only(T.init));
    /// ditto
    PType params;
    static if(hasValidation)
        /// if false, valid indicates the callee should ignore this call.
        bool valid = true;
}

// conditionally add a join to a `Joins` item. This first ensures that the join
// doesn't already exist before adding it.
//
// If the TableDef doesn't have any dependencies, then it's added. Otherwise,
// the dependencies are added before the table definition itself.
//
// Note that Joins allows parameters, but TableDef does not.
package void addJoin(Item)(ref Joins!Item join, const TableDef def)
{
    if(join.hasJoin(def))
        // already added
        return;

    // TODO: see how we can possibly detect cycles

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

// encapsulate the updating of a specific field inside a query.
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

/**
 * Add columns to fetch to a specific query. Note that this is a separate
 * function from creating a new query because it requires knowing what the `Item`
 * type is.
 *
 * Params:
 *    Q - the query type. Must be a query according to sqlbuilder.traits.isQuery
 *    Cols - the columns to add. A column type must have an `expr` member, and
 *         optionally can provide table references and SQL parameters. Any
 *         types that are actually `Dataset` instances will be mapped to the
 *         type represented by the column. Otherwise, the type represented by
 *         the column is appended to the type list for the query (identifed by `column..
 *    query - The query. This is accepted by value because it will append data
 *            to the internal data without affecting the original.
 *    columns - The columns being passed.
 *
 * Returns:
 *    A query type that identifies which columns will be returned. The
 *    resulting query can be used in a dialect-specific fetch to map to actual
 *    types.
 */
auto select(Q, Cols...)(Q query, Cols columns) if (isQuery!Q)
{
    updateQueryField!true(query.fields, query.joins, columns);
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

/**
 * The special `changed` placeholder record a boolean that tells you whether
 * the column in question has changed from one row to the next. This is useful
 * when fetching related rows where the primary table is repeated for each row,
 * and you want to know when this has changed.
 *
 * As an example, imagine fetching a list of books and authors, and you are
 * storing an author along with an array of books inside the author. You might
 * want to know when the author has changed, such that you can start working
 * with a new author.
 *
 * Params: col - The column to check between rows to see if it's changed.
 *         ds - A dataset to check for changes. It must have a primary key.
 * Returns: A `ColumnDef` that has a specialized wrapper type to identify this
 *          is a column change detector.
 */
auto changed(T)(ColumnDef!T col)
{
    // create a column def based on the given column
    return ColumnDef!(Changed!T)(col.table, col.expr);
}

/// ditto
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

/**
 * Order a query by the provided expressions.
 *
 * Each expression is processed in the specified order, so specify the primary
 * sort column first, then secondary column next, etc.
 *
 * Use the `ascend` or `descend` modifiers to explicitly list whether ordering
 * should be ascending or descending.
 *
 * Params:
 *     query - The query to amend
 *     expressions - A set of columns/expressions to use for sorting.
 * Returns: An amended query adding the new sort column information.
 */
Q orderBy(Q, Expr...)(Q query, Expr expressions) if (isQuery!Q)
{
    updateQueryField!false(query.orders, query.joins, expressions);
    return query;
}

/**
 * Add a 'group by' clause to a query.
 *
 * SQL 'group by' allows one to use aggregation functions such as 'sum' or
 * 'average' and can ensure only one instance of a column per row. See specific
 * SQL documentation for details.
 *
 * Params:
 *     query - The query to amend.
 *     cols - The list of columns/expressions to use for grouping the resulting
 *     query.
 * Returns: The amended query.
 */
Q groupBy(Q, Cols...)(Q query, Cols cols) if (isQuery!Q)
{
    updateQueryField!false(query.groups, query.joins, cols);
    return query;
}

/**
 * Limit the number of rows returned in a query, and/or set the offset of where
 * to start in the list.
 *
 * Params:
 *     query - The query to amend.
 *     numItems - The maximum number of rows to fetch
 *     offset - How many rows to skip before starting the fetch.
 * Returns: The amended query.
 */
Q limit(Q)(Q query, size_t numItems, size_t offset = 0) if (isQuery!Q)
{
    query.limitQty = numItems;
    query.limitOffset = offset;
    return query;
}

/**
 * This specialized function can be used to update an SQLFragment representing
 * query conditions and joins with a list of new conditions.
 *
 * Each spec item is added to the list of expressions. When inside an aggregate
 * spec (`AND` or `OR`), the new spec list is prepended with a condition
 * separator. This separator and appropriate punctuation is decided by the
 * specific dialect when translating to SQL.
 *
 * This system allows building conditions piecemeal, and even optionally
 * including a condition spec based on whether all of the pieces are valid.
 *
 * By default, all conditions are joined using `AND`.
 *
 * TODO: need some examples
 *
 * Params:
 *     conditions - The SQLFragment representing the conditions to update.
 *     joins - The Joins item to update.
 *     spec - The specification of the items to add to the conditions. The spec
 *         is treated as a list of strings or ExprStrings and parameters. Any
 *         joins are added as needed. One should not use concatenation, but
 *         rather just pass all components as a list to the conditions.
 */
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

/**
 * Add conditions to a query, update, or delete statement.
 *
 * Params:
 *     query - The query, update, or delete statement to amend.
 *     spec - The items to add to the conditions. They are added using the
 *        `updateConditions` function. See that function for more details.
 * Returns: The amended statement.
 */
Q where(Q, Spec...)(Q query, Spec spec) if ((isQuery!Q || is(Q : Update!T, T) || is(Q : Delete!T, T)) && Spec.length > 0)
{
    updateConditions(query.conditions, query.joins, spec);
    return query;
}

/**
 * This function simplifies the conditional expression string of a statement
 * based on the grouping tokens. This will eliminate empty groupings (and the
 * separators surrounding it), and also remove extraneous groupings.
 *
 * If the term "NOT" (in any captialization/spacing) is detected before the
 * removed grouping, it is considered part of the grouping, and also removed.
 *
 * It also uses boolean logic to remove extraneous conditions (like FALSE AND
 * someCondition will remove someCondition). It will NOT remove joins that are
 * no longer needed, due to the condition being removed, so you still must deal
 * with possible group by clauses.
 *
 * This function rewrites the expression array and possibly the parameter
 * array in-place, which means that you should only use this function when you
 * know there is only one reference to this data.
 *
 * It accepts the aggregate by reference, and returns a reference to it at the
 * end.
 *
 * Params: query - Query, Update, or Delete statement to simplify the
 *             conditions for.
 * Returns: The referenced statement, with simplified conditions.
 */
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
            import std.conv;
            result.bidx = eidx == 0 ? eidx : eidx - 1;
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

/**
 * Generate an expression column.
 *
 * An expression column is similar to a normal column, but instead of using a
 * standard table column, it can be any expression, including data parameters.
 *
 * A current limitation is that because a ColumnDef only can represent one
 * table, expressions between 2 different tables will not work. This limitation
 * may be relaxed in the future.
 *
 * Params:
 *     T - The type that the expression column is expected to return from the
 *         database. This is backend-specific, and will not necesssarily work
 *         for all backends.
 *     args - The list of strings and expressions that will be used to generate
 *         the column. To name the column, use the `as` function.
 * Returns: A ColumnDef that represents the expression and given type.
 */
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

/**
 * Rename a specific column into a different name. Note that this is only for
 * usage outside sqlbuilder, as this library only uses column names when
 * serializing row types.
 *
 * Using this function more than once will result in an error in the resulting SQL.
 *
 * Params:
 *     col - The column to rename, or concatenation to rename.
 *     newName - The new name to use.
 * Returns:
 *     A new ColumnDef or ConcatDef with the SQL to express the rename.
 */
ColumnDef!T as(T)(ColumnDef!T col, string newName)
{
    return exprCol!T(col, " AS ", newName.makeSpec(Spec.id));
}

/// ditto
ConcatDef as(ConcatDef col, string newName)
{
    return ConcatDef(col.tables, col.expr ~ " AS " ~ newName.makeSpec(Spec.id));
}

/**
 * Perform the aggregate function `COUNT` on a specified column.
 *
 * Note that the column type is always specified as `long`.
 *
 * Params: col - ColumnDef used to count.
 * Returns: A new ColumnDef which is the count of the column id.
 */
ColumnDef!long count(T)(ColumnDef!T col)
{
    return exprCol!long("COUNT(", col, ")");
}

/**
 * Used to add an Ascending order directive to a column for orderBy.
 *
 * Params: col - The column to order ascending
 * Returns: A new ColumnDef with the appropriate directive.
 */
ColumnDef!T ascend(T)(ColumnDef!T col)
{
    return ColumnDef!T(col.table, col.expr ~ " ASC");
}

/**
 * Used to add a Descending order directive to a column for orderBy.
 *
 * Params: col - The column to order descending
 * Returns: A new ColumnDef with the appropriate directive.
 */
ColumnDef!T descend(T)(ColumnDef!T col)
{
    return ColumnDef!T(col.table, col.expr ~ " DESC");
}

/**
 * Instruct SQL to concatenate expressions together to generate an expression column.
 *
 * The resulting column is always a string, and can be used in sorts, fetches,
 * orders, etc.
 *
 * Params: args - The expression to concatenate.
 * Returns: A ConcatDef which represents the concatenation expression.
 */
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

/**
 * The base implementation of SQLBuilder dialects using a specific item type. A
 * dialect should use this template by instantiating it, and publicly aliasing
 * all members into the module space. See existing dialect modules for how to
 * do this.
 *
 * This is specifically not a mixin template because we want dialects with
 * common Item types to use equivalent types and functions.
 */
template SQLImpl(Item, alias param)
{

    /**
     * Generate a query for selecting columns from the database. This version
     * sets up a new query, and then calls the more general select function.
     *
     * Params:
     *    Cols - the columns to add. A column type must have an `expr` member, and
     *         optionally can provide table references and SQL parameters. Any
     *         types that are actually `Dataset` instances will be mapped to the
     *         type represented by the column. Otherwise, the type represented by
     *         the column is appended to the type list for the query (identifed by `column..
     *    columns - The columns being passed.
     *
     * Returns:
     *    A query type that identifies which columns will be returned. The
     *    resulting query can be used in a dialect-specific fetch to map to actual
     *    types.
     */
    auto select(Cols...)(Cols cols) if (cols.length == 0 || !isQuery!(Cols[0]))
    {
        return select(Query!Item(), cols);
    }

    alias select = sqlbuilder.dialect.common.select;

    /**
     * Generate an insert statement for a specified table.
     *
     * An insert statement uses a specified table (which cannot be a joined
     * table) to create an SQL statement that inserts values into a table.
     *
     * Use the `set` function to set values in an inserted row.
     *
     * Params:
     *    table - The table to insert a row for.
     *
     * Returns:
     *    An insert statement that can be executed for a specific backend.
     */
    Insert!Item insert(const(TableDef) table)
    {
        if(table.dependencies.length)
            throw new Exception("Cannot insert into a joined table: " ~ table.as);
        return Insert!Item(table.as);
    }

    /**
     * Set a field for a row in an insert statement.
     *
     * Params:
     *    ins - The insert statement begin amended
     *
     * Returns:
     *    The augmented insert statement.
     */
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

    /**
     * Create an insert statement to create a row based on a model structure.
     *
     * Params:
     *    item - The item to insert. The statement is based on the values of
     *         this item. Any autoincrement field is ignored.
     *
     * Returns:
     *    An insert statement that if executed will insert this item.
     */
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

    /**
     * Create an Update statment to set a column to a specified value. The
     * value must be a valid parameter type (use the `.param` or `.optional`
     * function in a specific dialect to wrap any existing value).
     *
     * Params:
     *
     *     column - The column to set. Generally this is a `ColumnDef`
     *     value - The value to set the column to.
     *     upd - The existing update statement to amend.
     *
     * Returns:
     *     An update statement that will update the specified column to the
     *     specified field.
     */
    Update!Item set(Col, Val)(Col column, Val value)
    {
        Update!Item result;
        return set(result, column, value);
    }

    /// ditto
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

    /**
     * Shortcut to update all the fields in a row.
     *
     * Params:
     *     item - The row item to update. Any primary key data is used as a
     *         condition for the update instead of being updated directly.
     *
     * Returns:
     *     An update statement that can be executed to perform the update.
     */
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

    /**
     * Set up a delete SQL statement for a specified table. The table must be a
     * primary table and not joined.
     *
     * Params: table - The table to delete from
     * Returns: A Delete statement which will remove rows from the specified table.
     */
    Delete!Item removeFrom(const TableDef table)
    {
        if(table.dependencies.length)
            throw new Exception("Cannot delete from a joined table: " ~ table.as);
        Delete!Item result;
        result.joins.addJoin(table);
        return result;
    }

    /**
     * Create a Delete statement that removes the specified item.
     *
     * The item must have a primary key defined.
     *
     * Params: item - The model item to remove. Only the primary key fields are
     *             used for the statement.
     * Returns: A Delete statement that will remove the matching row.
     */
    Delete!Item remove(T)(T item) if (hasPrimaryKey!T)
    {
        import sqlbuilder.dataset;
        DataSet!T ds;
        return removeFrom(ds.tableDef).havingKey(ds, item);
    }

    /**
     * Amend the conditions of a statement based on the key values from a
     * given model.
     *
     * Params:
     *      query - The statement to amend.
     *      t - A DataSet for an item that has a primary key. Note that this is
     *         needed in case the DataSet is not the primary table, but a joined
     *         table.
     *      model - The model to use for key values.
     * Returns: The amended query.
     */
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
