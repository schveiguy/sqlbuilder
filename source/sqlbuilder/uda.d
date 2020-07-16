/// UDA items to apply to row structures
module sqlbuilder.uda;

struct tableName
{
    string name;
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

// specify a different column name for a field. By default, the field's name is
// used.
struct colName
{
    string name;
}

// specify an alternate column type. This is possibly specific to the database
// engine. By default the engine picks the type based on the field type. This
// only has any bearing when creating tables. This does NOT affect parameter
// types, and should still be something that properly serializes to/from the
// given D type. For example, a string field could be adjusted to be VARCHAR(10)
// instead of TEXT. Use a specialized type to convert between different D
// types.
struct colType
{
    string type;
}

// ignore a field, it is not considered part of the database data.
enum ignore;

// tag a column as part of the primary key
enum primaryKey;

// make a specific column unique in the table
enum unique;

// tag a column as receiving an automatic value incremented by the database
// (usually the id).
enum autoIncrement;

// TODO: figure out how to do indexes besides primary key.
