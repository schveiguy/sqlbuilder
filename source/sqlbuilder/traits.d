module sqlbuilder.traits;
import sqlbuilder.uda;
import sqlbuilder.types;

private struct SingleTableDef
{
    @safe pure @nogc nothrow:
    const(TableDef) _front;
    bool _empty = false;
    auto front() { return _front; }
    void popFront() { _empty = true; }
    bool empty() { return _empty; }
}

// get any dependent tables for an item, even if the item doesn't have any
package auto ref getTables(T)(auto ref T item)
{
    import std.range.primitives;
    static if(is(typeof({ return item.tables;}()) R) && isInputRange!R && is(ElementType!R : const TableDef))
    {
        return item.tables;
    }
    else static if(__traits(hasMember, T, "tables"))
    {
        static assert(0, "Type " ~ T.stringof ~ " has a member `tables`, but does not provide the correct type");
    }
    else static if(is(typeof({ return item.table;}()) : const(TableDef)))
    {
        return SingleTableDef(item.table);
    }
    else static if(__traits(hasMember, T, "table"))
    {
        static assert(0, "Type " ~ T.stringof ~ " has a member `table`, but does not provide the correct type");
    }
    else
        return TableDef[].init;
}

// gets the table name of a given type. This looks for the TableName UDA or
// just uses the struct's name otherwise.
template getTableName(T)
{
    static foreach(u; __traits(getAttributes, T))
        static if(is(typeof(u) == tableName))
            enum result = u.name;
    static if(is(typeof(result)))
        enum getTableName = result;
    else
        enum getTableName = T.stringof;
}

template isField(T, string item)
{
    private import std.traits;
    // is a member, is not a static member, is not a Relation type and not ignored.
    // TODO: we ignore functions for now, but possibly we may want to include them.
    static if(__traits(hasMember, T, item))
    {
        enum isField = !is(__traits(getMember, T, item)) &&
            !__traits(compiles, () {
                auto x = __traits(getMember, T, item);
            }) &&
            !hasUDA!(__traits(getMember, T, item), ignore) &&
            !is(typeof(__traits(getMember, T, item)) == Relation) &&
            !is(typeof(__traits(getMember, T, item)) == function);
            /*item != "opAssign" &&
            item != "opCmp" &&
            item != ;*/
    }
    else
        enum isField = false;
}

// gets the field that contains the given relation.
template getRelationField(T, string item)
{
    private import std.traits;
    static if(__traits(hasMember, T, item) && is(typeof(__traits(getMember, T, item)) == Relation))
    {
        // A relation field directly in the item
        enum getRelationField = item;
    }
    else
    {
        // look for a field with a UDA
        static foreach(f; __traits(allMembers, T))
            static foreach(u; __traits(getAttributes, __traits(getMember, T, f)))
            {
                static if(is(typeof(u)) && isInstanceOf!(refersTo, typeof(u)) && u.name == item)
                    enum result = f;
            }
        static if(is(typeof(result)))
            enum getRelationField = result;
        else
            enum string getRelationField = null;
    }
}

// get a relation in T that has a relation to a U
template getRelationField(T, U)
{
    private import std.traits;
    // look for a field with a UDA
    static foreach(f; __traits(allMembers, T))
        static foreach(u; __traits(getAttributes, __traits(getMember, T, f)))
        {
            static if(is(typeof(u)) && isInstanceOf!(refersTo, typeof(u)) && is(u.foreign_table == U))
            {
                static if(is(typeof(result)))
                    static assert(0, "Multiple relations exist for " ~ T.stringof ~ " to " ~ U.stringof);
                else
                    enum result = f;
            }
        }
    static if(is(typeof(result)))
        enum getRelationField = result;
    else
        enum string getRelationField = null;
}

enum isRelation(T, string item) = getRelationField!(T, item) != null;

template getRelationFor(alias sym)
{
    import std.traits : isInstanceOf;
    static foreach(u; __traits(getAttributes, sym))
    {
        static if(is(typeof(u) : refersTo!T, T))
        {
            static if(u.name == null)
                // make a copy with the name replaced
                enum result = () {
                    auto x = u;
                    x.name = __traits(identifier, sym);
                    return x;
                } ();
            else
                enum result = u;
        }
        static if(is(u : refersTo!T, T))
            enum result = u(__traits(identifier, sym));
        static if(isInstanceOf!(mustReferTo, u))
            enum result = u(__traits(identifier, sym));
    }
    static if(is(typeof(result)))
        enum getRelationFor = result;
    else
        alias getRelationFor = void;
}

enum isRelationField(alias sym) = is(typeof(getRelationFor!sym) == refersTo!U, U);

template getMappingsFor(alias sym)
{
    import std.meta;
    template m_list(Params...)
    {
        static if(Params.length == 0)
            alias m_list = AliasSeq!();
        else static if(is(typeof(Params[0]) == mapping))
        {
            static if(is(typeof(sym) == Relation))
                enum result = Params[0];
            else
            {
                // mapping attached to a field, use the name of the field
                // instead of the key.
                static if(isKeyLiteral(Params[0].key))
                    enum result = Params[0];
                else
                    // ignore any name there, just use the actual field name.
                    enum result = mapping(Params[0].foreign_key, __traits(identifier, sym));
            }
            alias m_list = AliasSeq!(result, m_list!(Params[1 .. $]));
        }
        else
            alias m_list = m_list!(Params[1 .. $]);
    }
    alias result = m_list!(__traits(getAttributes, sym));
    static if(result.length > 0)
        enum getMappingsFor = result;
    else
    {
        // no mapping UDA
        // for Relations, assume both keys are the default. For non-relations,
        // use the field name as the local key.
        static if(is(typeof(sym) == Relation))
            alias getMappingsFor = AliasSeq!(mapping.init);
        else
            enum getMappingsFor = AliasSeq!(mapping(mapping.init.foreign_key, __traits(identifier, sym)));
    }
}

template getParamType(T...)
{
    static if(T.length == 0)
        alias getParamType = void;
    else static if(T.length == 1)
    {
        alias X = T[0];
        static if(is(typeof({X x = X.init; return x.params;}()) P))
        {
            import std.range : ElementType;
            alias getParamType = ElementType!P;
        }
        else
        {
            alias getParamType = void;
        }
    }
    else
    {
        alias P1 = getParamType!(T[0]);
        alias P2 = getParamType!(T[1 .. $]);
        static if(is(P1 == void))
            alias getParamType = P2;
        else static if(is(P2 == void))
            alias getParamType = P1;
        else
            alias getParamType = typeof(true ? P1.init : P2.init);
    }
}

template getColumnName(alias sym)
{
    static foreach(u; __traits(getAttributes, sym))
        static if(is(typeof(u) == colName))
            enum result = u.name;
    static if(is(typeof(result)))
        enum getColumnName = result;
    else
        enum getColumnName = __traits(identifier, sym);
}

// TODO: use some means to detect custom datasets.
template isDataSet(T)
{
    import std.traits : isInstanceOf;
    import sqlbuilder.dataset;
    enum isDataSet = isInstanceOf!(DataSet, T);
}

template isQuery(T)
{
    import std.traits : isInstanceOf;
    enum isQuery = isInstanceOf!(Query, T);
}

template getQueryTypeList(Q, Cols...)
{
    import std.meta : AliasSeq;
    static if(Q.QueryTypes.length == 1 && is(Q.QueryTypes[0] == void))
        alias getQueryTypeList = AliasSeq!(void);
    else
    {
        template helper(size_t idx, X...)
        {
            static if(idx >= X.length)
                alias helper = X;
            else static if(isDataSet!(X[idx]))
                alias helper = helper!(idx + 1, X[0 .. idx], typeof(X[idx].allColumns()).type, X[idx + 1 .. $]);
            else static if(is(X[idx].type) && !is(X[idx].type == void))
            {
                alias helper = helper!(idx + 1, X[0 .. idx], X[idx].type, X[idx + 1 .. $]);
            }
            else
                alias helper = AliasSeq!(void);
        }
        alias newTypes = helper!(0, Cols);
        static if(newTypes.length == 1 && is(newTypes[0] == void))
            alias getQueryTypeList = AliasSeq!(void);
        else
            alias getQueryTypeList = AliasSeq!(Q.QueryTypes, newTypes);
    }
}

template primaryKeyFields(T)
{
    import std.meta : AliasSeq;
    import std.traits : hasUDA;
    template PKHelper(Fields...)
    {
        static if(Fields.length == 0)
            alias PKHelper = AliasSeq!();
        else
        {
            static if(hasUDA!(__traits(getMember, T, Fields[0]), primaryKey))
                alias PKHelper = AliasSeq!(Fields[0], PKHelper!(Fields[1 .. $]));
            else
                alias PKHelper = PKHelper!(Fields[1 .. $]);
        }
    }

    alias primaryKeyFields = PKHelper!(__traits(allMembers, T));
}

enum hasPrimaryKey(T) = primaryKeyFields!T.length > 0;

template PrimaryKeyTypes(T)
{
    import std.meta : AliasSeq;
    template PKHelper(Fields...)
    {
        static if(Fields.length == 0)
            alias PKHelper = AliasSeq!();
        else
            alias PKHelper = AliasSeq!(typeof(__traits(getMember, T, Fields[0])), PKHelper!(Fields[1 .. $]));
    }

    alias PrimaryKeyTypes = PKHelper!(primaryKeyFields!T);
}

template possibleNullColumn(alias sym)
{
    import std.typecons : Nullable;
    static if(is(typeof(sym) : Nullable!T, T))
        enum possibleNullColumn = true;
    else static if(is(getAllowNullType!sym == AllowNullType!Args, Args...))
        enum possibleNullColumn = true;
    else
        enum possibleNullColumn = false;
}

template getAllowNullType(alias sym)
{
    // get the null type, which includes the default value if it's null
    static foreach(alias att; __traits(getAttributes, sym))
    {
        static if(__traits(isSame, att, allowNull))
            alias result = AllowNullType!(typeof(sym), sym.init);
        else static if(is(typeof(att) : AllowNull!T, T))
                                        alias result = AllowNullType!(typeof(sym), att.nullValue);
    }
    // if we didn't alias, then allowNull isn't present.
    static assert(is(result), "No allowNull UDA detected on symbol " ~ __traits(identifier, sym));
    alias getAllowNullType = result;
}

template getFetchType(T)
{
    static if(is(T == AllowNullType!Args, Args...))
        alias getFetchType = T.type;
    else static if(is(T == RowObj!U, U))
        alias getFetchType = U;
    else
        alias getFetchType = T;
}


unittest
{
    import std.typecons : Nullable;
    static struct TestRow
    {
        Nullable!int n1;
        @allowNull int n2;
        @allowNull(5) int n3;
        int v1;
    }

    static assert(possibleNullColumn!(TestRow.n1));
    static assert(possibleNullColumn!(TestRow.n2));
    static assert(possibleNullColumn!(TestRow.n3));
    static assert(!possibleNullColumn!(TestRow.v1));

    static assert(is(getAllowNullType!(TestRow.n2) == AllowNullType!(int, 0)));
    static assert(is(getAllowNullType!(TestRow.n3) == AllowNullType!(int, 5)));
    static assert(!__traits(compiles, getAllowNullType!(TestRow.n1)));
    static assert(!__traits(compiles, getAllowNullType!(TestRow.v1)));
}
