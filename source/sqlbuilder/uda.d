/**
 * UDA items for creating model structures for your datasets and for table
 * creation.
 *
 * Copyright: 2021 Steven Schveighoffer
 * License: Boost-1.0, see LICENSE.md
 */
module sqlbuilder.uda;
import sqlbuilder.types : Spec;

/**
 * Tag your structure to provide an alternative table name. Without this, the
 * struct's name is used as the table name.
 */
struct tableName
{
    string name;
}

///
unittest
{
    import sqlbuilder.uda;
    import sqlbuilder.dialect.mysql;
    import sqlbuilder.dataset;

    @tableName("trec") static struct TableRecord
    {
        int id;
    }
    DataSet!TableRecord ds;
    assert(select(ds).where(ds.id, " = ", 5.param).sql ==
           "SELECT `trec`.* FROM `trec` WHERE (`trec`.`id` = ?)");
}


///
struct AllowNull(T)
{
    T nullValue;
}

/**
 * Allow null for a specific field that is not typed as Nullable!T.
 *
 * One can either assign a value to use as the NULL value, or simply tag a
 * member with the @allowNull function alias. In the latter case, the .init
 * value of the column type is used as the NULL value.
 *
 * This attribute ONLY applies on reading data. Inserting data using a
 * blueprint or model will not translate the NULL value sentinel to an insert
 * of NULL. However, using standard `set` calls, you can still set the value to
 * NULL.
 */
AllowNull!T allowNull(T)(T val)
{
    return AllowNull!T(val);
}

/**
 * This attribute identifies the table that this column or relation refers to.
 * Use the `mustReferTo` form to more intuitively indicate a strong relation
 * (rather than pass a `true` as a third parameter to `refersTo`).
 *
 * The alias `foreign_table` identifies the other table type that this
 * identifier refers to.
 *
 * The name identifies the name that should be used for a column only. A
 * relation tagged with this UDA will ignore the name field.
 *
 * The joinType field identifies the join that should be used. By default, all
 * joins are `LEFT` joins so as to not affect the data already selected by the
 * join. You can override the join type when using the relation from a dataset.
 *
 * The strong field identifies that the relatable row must exist in the foreign
 * table.
 */
struct refersTo(T)
{
    alias foreign_table = T;
    string name;
    Spec joinType = Spec.none;
    bool strong;
}

/// ditto
refersTo!T mustReferTo(T)(string name = null, Spec joinType = Spec.none)
{
    return refersTo!T(name, joinType, true);
}

/**
 * This defines a mapping for a relationship. Each item can either represent a
 * field in the foreign table or a literal string to use as the mapping. The
 * library distinguishes between the two by checking to see if the key could be
 * a valid identifier. Typically, literals are not valid identifiers in any
 * SQL.
 *
 * If one key is an identifier, and the other is a literal, then the identifier
 * is always provided first, to allow any relationship to be expressed.
 *
 * The default key is "id", as this is the most common name to map tables with.
 *
 * Only when both fields are identifiers is the '=' operator inserted.
 * Otherwise, the operator must be provided in the literal portion.
 *
 * If the key field is an identifier, and the mapping attribute is attached to
 * an actual column, the field is ignored, and the column name is used instead
 * for the local field. Relations that need multiple column mappings should be
 * specified as Relation types.
 *
 * Examples (assuming ftable = foreign table and ltable = local table):
 *
 * mapping("foo", "bar") => ftable.foo = ltable.bar
 * mapping("foo") => ftable.foo = ltable.id (or ftable.foo = ltable.field when this is attached to a field)
 * mapping("foo", "IS NULL") => ftable.foo IS NULL
 * mapping("foo", "= 7") => ftable.foo = 7
 *
 * note the following reverse the order of expression
 * mapping("IS NULL", "bar") => ltable.bar IS NULL
 * mapping(" = 1", "bar") => ltable.bar = 1
 *
 * mapping("1", " = 0") => 1 = 0 (i.e. never match)
 */
struct mapping
{
    string foreign_key = "id";
    string key = "id";
}

///
unittest
{
    import sqlbuilder.uda;
    import sqlbuilder.types;
    import sqlbuilder.dataset;
    import sqlbuilder.dialect.mysql;

    // foreign table
    static struct FT
    {
        int id;
        int foo;
    }

    // local table
    static struct LT
    {
        int id;
        @refersTo!FT("byColumnMap") @mapping("foo") int bar;
        @refersTo!FT @mapping("foo", "bar") Relation byFieldMap;
        @refersTo!FT @mapping("foo") Relation byFooWithId;
        @refersTo!FT @mapping("foo", "IS NULL") Relation byFooIsNull;
        @refersTo!FT @mapping("foo", "= 42") Relation byFoo42;
        @refersTo!FT @mapping("IS NULL", "bar") Relation byBarIsNull;
        @refersTo!FT @mapping("= 1", "bar") Relation byBar1;
        @refersTo!FT @mapping("1", "= 0") Relation noMatch;
    }

    DataSet!LT ds;
    assert(select(ds.byColumnMap).sql ==
           "SELECT `LT_L_byColumnMap`.* FROM `LT` LEFT JOIN `FT` AS `LT_L_byColumnMap` ON (`LT_L_byColumnMap`.`foo` = `LT`.`bar`)");
    assert(select(ds.byFieldMap).sql ==
           "SELECT `LT_L_byFieldMap`.* FROM `LT` LEFT JOIN `FT` AS `LT_L_byFieldMap` ON (`LT_L_byFieldMap`.`foo` = `LT`.`bar`)");
    assert(select(ds.byFooWithId).sql ==
           "SELECT `LT_L_byFooWithId`.* FROM `LT` LEFT JOIN `FT` AS `LT_L_byFooWithId` ON (`LT_L_byFooWithId`.`foo` = `LT`.`id`)");

    assert(select(ds.byFooIsNull).sql ==
           "SELECT `LT_L_byFooIsNull`.* FROM `LT` LEFT JOIN `FT` AS `LT_L_byFooIsNull` ON (`LT_L_byFooIsNull`.`foo` IS NULL)");
    assert(select(ds.byFoo42).sql ==
           "SELECT `LT_L_byFoo42`.* FROM `LT` LEFT JOIN `FT` AS `LT_L_byFoo42` ON (`LT_L_byFoo42`.`foo` = 42)");
    assert(select(ds.byBarIsNull).sql ==
           "SELECT `LT_L_byBarIsNull`.* FROM `LT` LEFT JOIN `FT` AS `LT_L_byBarIsNull` ON (`LT`.`bar` IS NULL)");
    assert(select(ds.byBar1).sql ==
           "SELECT `LT_L_byBar1`.* FROM `LT` LEFT JOIN `FT` AS `LT_L_byBar1` ON (`LT`.`bar` = 1)");
    assert(select(ds.noMatch).sql ==
           "SELECT `LT_L_noMatch`.* FROM `LT` LEFT JOIN `FT` AS `LT_L_noMatch` ON (1 = 0)");
}

/**
 * Specify a different column name for a field. By default, the field's name is
 * used.
 */
struct colName
{
    string name;
}

///
unittest
{
    import sqlbuilder.dataset;
    import sqlbuilder.uda;
    import sqlbuilder.dialect.mysql;
    static struct table
    {
        @colName("password_hash") string pwHash;
    }

    DataSet!table ds;
    assert(select(ds.pwHash).sql ==
           "SELECT `table`.`password_hash` FROM `table`");
}

/**
 * Specify an alternate column type. This is possibly specific to the database
 * engine. By default the engine picks the type based on the field type. This
 * only has any bearing when creating tables.
 *
 * This does NOT affect parameter types, and should still be something that
 * properly serializes to/from the given D type. For example, a string field
 * could be adjusted to be VARCHAR(10) instead of TEXT. Use a specialized type
 * to convert between different D types.
 */
struct colType
{
    string type;
}

///
unittest
{
    import sqlbuilder.uda;
    import sqlbuilder.dialect.mysql;
    static struct table
    {
        @colType("VARCHAR(10)") string name;
    }

    assert(createTableSql!table ==
           "CREATE TABLE `table` (`name` VARCHAR(10) NOT NULL)");
}

/**
 * Ignore a field, it is not considered part of the database data.
 *
 * This is useful for fields that only exist on the record in D, but not on the
 * database. For instance a calculated field that is not in the database, or a
 * network connection associated with a record.
 */
enum ignore;

/**
 * Tag a column as part of the primary key.
 *
 * The primary key is used when generating keys for a type, and also for
 * updating a record. sqlbuilder does not treat any fields by default as the
 * primary key.
 */
enum primaryKey;

/**
 * Mark a specific column as unique on the table. This only is used when
 * generating the table and is not (currently) used in any queries or
 * executions.
 */
enum unique;

/**
 * Tag a column as receiving an automatic value incremented by the database
 * (usually the id).
 *
 * When inserting whole items into the table, any autoincrement columns are
 * given the value returned from the database.
 *
 * In many cases, database implementations only allow one autoIncrement value
 * to be returned. In this case, the autoIncrement value will be assigned to
 * ALL items in the model, even if they are different.
 */
enum autoIncrement;

// TODO: figure out how to do indexes besides primary key.
