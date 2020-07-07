/// UDA items to apply to row structures
module sqlbuilder.uda;

struct TableName
{
    string name;
    alias name this;
}

TableName tableName(string s)
{
    return TableName(s);
}

enum RefType
{
    OneToMany,
    ManyToOne,
    OneToOne
}

struct TableReference(T)
{
    alias foreign_table = T;
    string name;
    RefType type;
}

TableReference!T oneToMany(T)(string name = null)
{
    return TableReference!T(name, RefType.OneToMany);
}

TableReference!T manyToOne(T)(string name = null)
{
    return TableReference!T(name, RefType.ManyToOne);
}

TableReference!T oneToOne(T)(string name = null)
{
    return TableReference!T(name, RefType.OneToOne);
}

struct mapping
{
    string foreign_key = "id";
    // can leave off if "id" is the key or if applied to a normal column
    string key = "id";
}

struct colName
{
    string name;
}

struct dbType
{
    string type;
}


enum indexed;

enum primaryKey;

enum unique;

enum autoIncrement;

