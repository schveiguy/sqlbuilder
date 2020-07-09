module sqlbuilder.types;
import sqlbuilder.traits;

enum Spec : char
{
    none = '\0',
    id = 'i',
    tableid = 't',
    param = 'p',
    join = 'j',
    objend = 'o', // denotes the end of an object
}

enum joinSpec = "\\" ~ Spec.join;

enum paramSpec = "\\" ~ Spec.param;

Spec getSpec(const(char)[] s)
{
    if(s.length >= 2 && s[0] == '\\')
    {
        return cast(Spec)(s[1]);
    }
    return Spec.none;
}

inout(char)[] getData(inout(char)[] s)
{
    if(getSpec(s) == Spec.none)
        return s;
    return s[2 .. $];
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
}

string makeSpec(string value, Spec spec)
{
    return "\\" ~ spec ~ value;
}

struct TableDef
{
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
    // track which tables have been added to the join list.
    // TODO: figure out how to do this without an AA.
    bool[string] tables;
}

// a query is a dynamic structure designed to contain all the things needed to
// generate an SQL query. The function sql will fetch the current query
// string based on the Dialect.
struct Query(Item, RowT...)
{
    SQLFragment!(Item) fields;
    SQLFragment!(Item) conditions;
    SQLFragment!(Item) orders;
    Joins!Item joins;

    // used by the serialization system to determine which rows this will
    // fetch. This is only valid if fetch was used to generate the query.
    alias RowTypes = RowT;

    // convenience to avoid having to use traits tricks.
    package alias ItemType = Item;

    // allow forgetting all the row types.
    static if(RowT.length > 1 || (RowT.length == 1 && !is(RowT[0] == void)))
    {
        ref .Query!Item basicQuery() return @trusted
        {
            return *cast(.Query!Item*)&this;
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

struct ColumnDef(T)
{
    const TableDef table;
    ExprString expr;
    alias type = T;
}

// basic expression for strings. Used to provide literal SQL to ExprString.
struct Expr
{
    string expr;
}
