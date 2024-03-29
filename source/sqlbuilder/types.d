module sqlbuilder.types;
import sqlbuilder.traits;

@safe:

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
    
    private import std.range;
    ref ExprString opOpAssign(string op: "~", R)(R other) if (isInputRange!R && is(ElementType!R == string))
    {
        // append only non-empty strings
        foreach(s; other)
            if(s.length > 0)
                data ~= s;
        return this;
    }

    bool opCast(T: bool)()
    {
        return data.length > 0;
    }

    void toString(Out)(Out outputRange)
    {
        import std.format : formattedWrite;
        put(outputRange, "sql{");
        foreach(d; data)
        {
            auto spec = getSpec(d);
            if(spec != Spec.none)
            {
                put(outputRange, "@");
                formattedWrite(outputRange, "%s", spec);
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
    import std.meta : staticMap;
    SQLFragment!(Item) fields;
    SQLFragment!(Item) conditions;
    SQLFragment!(Item) groups;
    SQLFragment!(Item) orders;
    Joins!Item joins;
    size_t limitQty = 0;
    size_t limitOffset = 0;

    // used by the serialization system to determine which rows this will
    // fetch. This is only valid if fetch was used to generate the query.
    alias RowTypes = staticMap!(getFetchType, RowT);
    alias QueryTypes = RowT;

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

struct AllowNullType(T, T defaultVal)
{
    alias type = T;
    enum nullVal = defaultVal;
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

// this specialized struct is something that is used as a placeholder for a
// type that is actually a row. This happens when you select an entire row from
// the DB instead of a single column. This expects an _objEnd column to define
// where the row pieces stop.
// This never should be seen by user code.
package struct RowObj(T)
{
}

// basic expression for strings. Used to provide literal SQL to ExprString.
struct Expr
{
    string expr;
}

// Deserialization game plan:
//
// 1. A RowObj of a Nullable!T:
//    a. T.deserializeRowNull exists? use it
//    b. T.deserializeRow exists? use it, catch any exception and set to null
//    c. Process all fields in the struct, if any of the fields are null, but are not Nullable themselves, the whole object is null.
// 2. A RowObj of a non-Nullable T:
//    a. T.deserializeRow exists? use it
//    b. Process all fields in the struct.
// 3. Changes struct
//    a. do the deserialization, compare with previous version. Set up struct
//
// The rest are for SINGLE COLUMN types (leaf types):
// 4. Nullable!T
//    a. Is the value null? return Nullable!T.init
//    b. Else, deserialize T
// 5. T that has dbValue/fromDbValue
//    a. return T.fromDbValue;
// 6. other dialect specific hooked types (e.g. DateTime, values that can marshal to/from strings)
//    a. Deserialize according to the dialect rules.
// 7. primitive types:
//    a. Deserialize according to the dialect rules.
