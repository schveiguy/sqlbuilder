module sqlbuilder.types;
import sqlbuilder.traits;

enum Spec : char
{
    none = '\0',
    id = 'i',
    tableid = 't',
    param = 'p',
    leftJoin = 'L',
    innerJoin = 'I',
    rightJoin = 'R',
    outerJoin = 'O',
    objend = 'e', // denotes the end of an object
    // separator between clauses.
    separator = ';',

    // clause starts and ends. Using these can allow for nested ands or ors
    beginAnd = '&',
    beginOr = '|',
    endGroup = '$',
}

enum joinSpec = "\\" ~ Spec.leftJoin;

enum paramSpec = "\\" ~ Spec.param;

enum objEndSpec = "\\" ~ Spec.objend;

enum andSpec = "\\" ~ Spec.beginAnd;

enum orSpec = "\\" ~ Spec.beginOr;

enum sepSpec = "\\" ~ Spec.separator;

enum endGroupSpec = "\\" ~ Spec.endGroup;

Spec getSpec(const(char)[] s)
{
    if(s.length >= 2 && s[0] == '\\')
    {
        return cast(Spec)(s[1]);
    }
    return Spec.none;
}

bool isJoin(Spec s)
{
    return s == Spec.leftJoin ||
        s == Spec.rightJoin ||
        s == Spec.innerJoin ||
        s == Spec.outerJoin;
}

bool isGroup(Spec s)
{
    return s == Spec.beginOr || s == Spec.beginAnd;
}

package bool isKeyLiteral(string val)
{
    // returns true if any character inside val cannot be an identifier
    if(val.length == 0)
        return false;
    import std.utf;
    import std.uni;
    auto checkme = val.byDchar;
    if(!(checkme.front == '_' || checkme.front.isAlpha))
        return true;
    checkme.popFront;
    foreach(c; checkme)
    {
        if(!(c == '_' || c.isAlphaNum))
            return true;
    }
    return false;
}

struct ExprString
{
    string[] data;

    this(const(string)[] input...)
    {
        data = input.dup;
    }

    this(string[] input)
    {
        data = input;
    }

    ExprString opBinary(string op: "~")(auto ref const(ExprString) other) const
    {
        return ExprString(data ~ other.data);
    }

    ExprString opBinary(string op: "~")(string other) const
    {
        if(other.length)
            return ExprString(data ~ other);
        return ExprString(data);
    }

    ExprString opBinaryRight(string op: "~")(string other) const
    {
        if(other.length)
            return ExprString(other ~ data);
        return ExprString(data);
    }

    ref ExprString opOpAssign(string op: "~")(auto ref const(ExprString) other)
    {
        if(other.data.length)
            data ~= other.data;
        return this;
    }

    ref ExprString opOpAssign(string op: "~")(string other)
    {
        // only add non-empty strings
        if(other.length)
            data ~= other;
        return this;
    }

    bool opCast(T: bool)()
    {
        return data.length > 0;
    }

    void toString(Out)(Out outputRange)
    {
        import std.range : put;
        import std.conv : to;
        put(outputRange, "sql{");
        foreach(d; data)
        {
            auto spec = getSpec(d);
            if(spec != Spec.none)
            {
                put(outputRange, "@");
                put(outputRange, spec.to!string);
                if(d.length > 2)
                {
                    put(outputRange, "(");
                    put(outputRange, d[2 .. $]);
                    put(outputRange, ")");
                }
            }
            else
                put(outputRange, d);
        }
        put(outputRange, "}");
    }

    // This function sanitizes and optimizes the expression string based on the
    // grouping tokens. This will eliminate empty groupings (and the separators
    // surrounding it), and also remove extraneous groupings.
    //
    // If the term "NOT" (in any captialization/spacing) is detected before the
    // grouping, it is considered part of the grouping, and also removed.
    //
    // This function rewrites the expression array, which means that you should
    // only use this function when you know there is only one reference to this
    // expression string.
    //
    // it returns `this` at the end for easier pipelining.
    //
    // TODO: constant fold TRUE or FALSE into either removing the rest of the
    // clause or removing the TRUE/FALSE. e.g.:
    // A AND TRUE => A
    // A AND FALSE => FALSE
    // A OR TRUE => TRUE
    // A OR FALSE => B
    ref ExprString sanitize() scope
    {
        import std.exception : enforce;
        static struct Result
        {
            size_t nTerms;
            size_t endidx;
        }

        static bool isNot(string s)
        {
            import std.string : strip, toUpper;
            import std.algorithm : equal;
            return equal(s.strip.toUpper, "NOT");
        }
        Result recurse(size_t eidx)
        {
            Spec myGroup = eidx == 0 ? Spec.none : getSpec(data[eidx - 1]);
            size_t nTerms = 0;
            bool expectTerm = true;
            Result singleGroup;
            size_t termStart = eidx; // to check for replacing our group with the subgroup.
loop:
            while(eidx < data.length)
            {
                auto s = getSpec(data[eidx]);
                with(Spec) switch(s)
                {
                case beginAnd:
                case beginOr:
                    {
                        if(expectTerm)
                            ++nTerms;
                        expectTerm = false;

                        auto subresult = recurse(eidx + 1);
                        if(subresult.nTerms == 0)
                        {
                            // remove all the terms from the blank item
                            data[eidx .. subresult.endidx + 1] = null;
                            // check if the prior item is a not
                            if(eidx > 0 && isNot(data[eidx - 1]))
                            {
                                // remove the preceeding NOT
                                data[--eidx] = "";
                            }

                            // see if we need to remove a separator
                            if(subresult.endidx + 1 < data.length && data[subresult.endidx + 1] == sepSpec)
                            {
                                data[subresult.endidx + 1] = "";
                                eidx = subresult.endidx + 1;
                            }
                            else
                            {
                                if(eidx > 0 && data[eidx - 1] == sepSpec)
                                    // remove the preceeding separator
                                    data[eidx - 1] = "";
                                eidx = subresult.endidx + 1;
                            }
                            --nTerms;
                            expectTerm = true;
                        }
                        else
                        {
                            // if the subgroup's terms are only 1 in length,
                            // there are no separators, and it's just one term.
                            if(subresult.nTerms == 1)
                            {
                                data[eidx] = "";
                                data[subresult.endidx] = "";
                            }
                            else if(getSpec(data[eidx]) == myGroup)
                            {
                                // this subgroup is the same type as the
                                // current group.
                                //
                                // make sure the previous item is not some
                                // arbitrary string (such as "NOT"), and then
                                // we can absorb the subgroup into our group.

                                // we only get here if myGroup is not Spec.none.
                                assert(eidx > 0);

                                if(getSpec(data[eidx - 1]) != none)
                                {
                                    data[eidx] = "";
                                    data[subresult.endidx] = "";
                                    nTerms += subresult.nTerms - 1;
                                }
                            }
                            if(data[eidx].length)
                            {
                                singleGroup = subresult;
                            }
                            eidx = subresult.endidx + 1;
                        }
                    }
                    break;
                case separator:
                    // another term is coming
                    enforce(!expectTerm, "Term expected, but got separator instead");
                    expectTerm = true;
                    ++eidx;
                    break;
                case endGroup:
                    // end of the group
                    break loop;
                default: // everything else comprises a `term`
                    if(expectTerm && data[eidx].length) // not removed
                    {
                        // this is a term
                        ++nTerms;
                        expectTerm = false;
                    }
                    ++eidx;
                    break;
                }
            }

            enforce(myGroup == Spec.none || eidx < data.length, "Invalid construction of groupings, end of data found");

            if(myGroup != Spec.none && nTerms == 1)
            {
                // find the first non-removed term. If it's a group, then we
                // hoist that group out to our group's info.
                assert(termStart != 0);
                foreach(i; termStart .. eidx)
                {
                    if(data[i].length)
                    {
                        auto s = getSpec(data[i]);
                        if(s == Spec.beginAnd || s == Spec.beginOr)
                        {
                            assert(singleGroup.endidx != 0);
                            assert(singleGroup.nTerms != 0);
                            // replace our group with this group
                            data[termStart - 1] = data[i];
                            data[i] = "";
                            data[singleGroup.endidx] = "";
                            nTerms = singleGroup.nTerms;
                        }
                        break;
                    }
                }
            }
            return Result(nTerms, eidx);
        }

        recurse(0);

        // remove all empty strings
        import std.algorithm : remove;
        data = data.remove!(s => s.length == 0);
        data.assumeSafeAppend;
        return this;
    }
}

unittest
{
    import std.stdio;
    static string idOf(string s)
    {
        return s.makeSpec(Spec.id);
    }
    assert(ExprString(andSpec, idOf("x"), " = 5", sepSpec, idOf("y"), " = 6", sepSpec, " NOT ", orSpec, andSpec, endGroupSpec, endGroupSpec, endGroupSpec).sanitize ==
           ExprString(andSpec, idOf("x"), " = 5", sepSpec, idOf("y"), " = 6", endGroupSpec));
    assert(ExprString(andSpec, idOf("x"), " = 5", endGroupSpec).sanitize ==
           ExprString(idOf("x"), " = 5"));
    assert(ExprString(andSpec, idOf("x"), " = 5", sepSpec, orSpec, idOf("y"), " = 6", endGroupSpec, endGroupSpec).sanitize ==
           ExprString(andSpec, idOf("x"), " = 5", sepSpec, idOf("y"), " = 6", endGroupSpec));
    assert(ExprString(andSpec, idOf("x"), " = 5", sepSpec, " NOT ", orSpec, idOf("y"), " = 6", endGroupSpec, endGroupSpec).sanitize ==
           ExprString(andSpec, idOf("x"), " = 5", sepSpec, " NOT ", idOf("y"), " = 6", endGroupSpec));
    assert(ExprString(andSpec, idOf("x"), " = 5", sepSpec, andSpec, idOf("y"), " = 6", sepSpec, idOf("z"), " = 7", endGroupSpec, endGroupSpec).sanitize ==
           ExprString(andSpec, idOf("x"), " = 5", sepSpec, idOf("y"), " = 6", sepSpec, idOf("z"), " = 7", endGroupSpec));
    assert(ExprString(andSpec, idOf("x"), " = 5", sepSpec, orSpec, andSpec, idOf("y"), " = 6", sepSpec, idOf("z"), " = 7", endGroupSpec, endGroupSpec, endGroupSpec).sanitize ==
           ExprString(andSpec, idOf("x"), " = 5", sepSpec, idOf("y"), " = 6", sepSpec, idOf("z"), " = 7", endGroupSpec));
    assert(ExprString(andSpec, idOf("x"), " = 5", sepSpec, orSpec, andSpec, endGroupSpec, sepSpec, andSpec, idOf("y"), " = 6", sepSpec, idOf("z"), " = 7", endGroupSpec, endGroupSpec, endGroupSpec).sanitize ==
           ExprString(andSpec, idOf("x"), " = 5", sepSpec, idOf("y"), " = 6", sepSpec, idOf("z"), " = 7", endGroupSpec));
}

void addSep(ref ExprString expr)
{
    if(expr)
    {
        auto lastSpec = expr.data[$-1].getSpec;

        if(lastSpec != Spec.beginAnd && lastSpec != Spec.beginOr)
            expr ~= sepSpec;
    }
}

string makeSpec(string value, Spec spec)
{
    return "\\" ~ spec ~ value;
}

string makeSpec(Spec spec)
{
    return "\\" ~ spec;
}

struct TableDef
{
    @property Spec joinType() const
    {
        if(joinExpr.data.length)
        {
            auto s = getSpec(joinExpr.data[0]);
            return s.isJoin ? s : Spec.none;
        }
        return Spec.none;
    }

    string as; // table name used in the expression
    ExprString joinExpr; // join expression defines the relationship and the table name
    const(TableDef)[] dependencies; // tables that must be included first
}

// used to designate a field as a relation (for when a relation doesn't have a
// dedicated field).
struct Relation
{
}


// An SQLFragment has an expression used to generate a portion of the SQL
// statement, along with a list of parameters that are used to pass custom data
// to the server.
struct SQLFragment(Item)
{
    ExprString expr;
    static if(!is(Item == void))
        Item[] params;
}

struct Joins(Item)
{
    SQLFragment!Item joinFragment;
    alias joinFragment this;

    bool hasJoin(const TableDef def)
    {
        // linear search. It's backwards because we only search for
        // dependencies if the leaf is not present. And leaves go on the
        // end.
        import std.algorithm : canFind;
        import std.range : retro;
        return joinFragment.expr.data.retro.canFind(def.joinExpr.data.retro);
    }
}

// a query is a dynamic structure designed to contain all the things needed to
// generate an SQL query. The function sql will fetch the current query
// string based on the Dialect.
struct Query(Item, RowT...)
{
    SQLFragment!(Item) fields;
    SQLFragment!(Item) conditions;
    SQLFragment!(Item) groups;
    SQLFragment!(Item) orders;
    Joins!Item joins;
    size_t limitQty = 0;
    size_t limitOffset = 0;

    // used by the serialization system to determine which rows this will
    // fetch. This is only valid if fetch was used to generate the query.
    alias RowTypes = RowT;

    // convenience to avoid having to use traits tricks.
    package alias ItemType = Item;

    // allow forgetting all the row types.
    static if(RowT.length == 0 || RowT.length > 1 || (RowT.length == 1 && !is(RowT[0] == void)))
    {
        ref .Query!(Item, void) basicQuery() return @trusted
        {
            return *cast(.Query!(Item, void)*)&this;
        }

        alias basicQuery this;
    }
    else
        ref Query basicQuery() return { return this; }
}

// UFCS method to fetch all the parameters from the given item.
package template paramsImpl(FieldNames...)
{
    string paramStr()
    {
        string result;
        foreach(n; FieldNames)
            result ~= "t." ~ n ~ ".params,";
        return result;
    }
    auto paramsImpl(T)(T t)
    {
        static if(is(t.ItemType == void))
        {
            import std.range : only;
            return only();
        }
        else
        {
            import std.range : chain;
            mixin("return chain(" ~ paramStr() ~ ");");
        }
    }
}

struct Insert(Item)
{
    alias ItemType = Item;

    // table for insertion.
    string tableid;

    // the items to set.
    SQLFragment!Item colNames;
    SQLFragment!Item colValues;
}

struct Update(Item)
{
    SQLFragment!Item settings;
    SQLFragment!Item conditions;
    Joins!Item joins;

    alias ItemType = Item;
}

struct Delete(Item)
{
    // relations, possibly used for "where" clause, but not deleted from
    Joins!Item joins;
    SQLFragment!Item conditions;

    alias ItemType = Item;
}

struct ColumnDef(T)
{
    const TableDef table;
    ExprString expr;
    alias type = T;
}

struct ConcatDef
{
    const(TableDef)[] tables;
    ExprString expr;
    alias type = string;
}

// a change struct
struct Changed(ColTypes...)
{
    ColTypes val;
    package bool _changed;

    bool opCast(T : bool)() { return _changed; }
}

// basic expression for strings. Used to provide literal SQL to ExprString.
struct Expr
{
    string expr;
}
