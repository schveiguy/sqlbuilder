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
