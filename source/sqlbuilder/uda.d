/// UDA items to apply to row structures
module sqlbuilder.uda;
import sqlbuilder.types : Spec;

struct tableName
{
    string name;
}

struct AllowNull(T)
{
    T nullValue;
}

// Allow null for a specific field that is not typed as Nullable!T.
//
// If val is provided, then it is used as the substitute for the null value. If
// not provided, (i.e. @allowNull is the attribute), then the init value of the
// column type is used instead.
//
// With this attribute, null values will never be inserted into the database
// using a blueprint or model. But it's still possible to set values to null.
// Create table statements will treat a column that has an allowNull attribute
// as nullable.
//
AllowNull!T allowNull(T)(T val)
{
    return AllowNull!T(val);
}

enum RefType
{
    One,
    Many,
}

struct TableReference(T)
{
    alias foreign_table = T;
    string name;
    Spec joinType = Spec.none;
}

TableReference!T refersTo(T)(string name = null, Spec joinType = Spec.none)
{
    return TableReference!T(name, joinType);
}

// this defines a mapping for a relationship. Each item can either represent a
// field in the foreign table or a literal string to use as the mapping. The
// library distinguishes between the two by checking to see if the key could be
// a valid identifier. Typically, literals are not valid identifiers in any
// SQL.
//
// If one key is an identifier, and the other is a literal, then the identifier
// is always provided first, to allow any relationship to be expressed.
//
// The default key is "id", as this is the most common name to map tables with.
//
// Only when both fields are identifiers is the '=' operator inserted.
// Otherwise, the operator must be provided in the literal portion.
//
// If the key field is an identifier, and the mapping attribute is attached to
// an actual column, the field is ignored, and the column name is used instead
// for the local field. Relations that need multiple column mappings should be
// specified as Relation types.
//
// Examples (assuming ftable = foreign table and ltable = local table):
//
// mapping("foo", "bar") => ftable.foo = ltable.bar
// mapping("foo") => ftable.foo = ltable.id (or ftable.foo = ltable.field when this is attached to a field)
// mapping("foo", "IS NULL") => ftable.foo IS NULL
// mapping("foo", "= 7") => ftable.foo = 7
//
// note the following reverse the order of expression
// mapping("IS NULL", "bar") => ltable.bar IS NULL
// mapping(" = 1", "bar") => ltable.bar = 1
//
// mapping("1", " = 0") => 1 = 0 (i.e. never match)
struct mapping
{
    string foreign_key = "id";
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
