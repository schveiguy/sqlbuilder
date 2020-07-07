module sqlbuilder.dialect.mysql;
public import sqlbuilder.dialect.common;
import sqlbuilder.types;
import sqlbuilder.traits;

string sql(QP...)(ref Query!(QP) q)
{
    import std.array : Appender;
    import std.range : put;
    Appender!string app;
    assert(q.fields.expr);
    assert(q.joins.expr);

    // add a set of fragments given the prefix and the separator
    void addFragment(SQLFragment!(q.ItemType) item, string prefix)
    {
        if(item.expr)
        {
            put(app, prefix);
            foreach(x; item.expr.data)
            {
                with(Spec) switch(getSpec(x))
                {
                case id:
                    put(app, '`');
                    put(app, x[2 .. $]);
                    put(app, '`');
                    break;
                case join: // TODO: implement other joins
                    put(app, " LEFT JOIN ");
                    break;
                case param:
                    put(app, '?');
                    break;
                case none:
                    put(app, x);
                    break;
                default:
                    throw new Exception("Unknown spec in: " ~ x);
                }
            }
        }
    }

    // fields
    addFragment(q.fields, "SELECT ");
    // joins
    addFragment(q.joins, " FROM ");
    // CONDITIONS
    addFragment(q.conditions, " WHERE ");
    // ORDER BY
    addFragment(q.orders, " ORDER BY ");

    return app.data;
}

private import std.meta : AliasSeq;
private import std.datetime : Date, DateTime, TimeOfDay;
private alias _typeMappings = AliasSeq!(
    byte, "TINYINT",
    short, "SMALLINT", 
    int, "INT",
    long, "BIGINT",
    ubyte, "TINYINT UNSIGNED", 
    ushort, "SMALLINT UNSIGNED",
    uint, "INT UNSIGNED",
    ulong, "BIGINT UNSIGNED",
    string, "TEXT",
    char[], "TEXT",
    const(char)[], "TEXT",
    ubyte[], "BLOB",
    const(ubyte)[], "BLOB",
    immutable(ubyte)[], "BLOB",
    float, "FLOAT",
    double, "DOUBLE",
    Date, "DATE",
    DateTime, "DATETIME",
    TimeOfDay, "TIME",
);


                            

// match the D type to a specific MySQL type
template getFieldType(T)
{
    static foreach(i; 0 .. _typeMappings.length / 2)
        static if(is(T == _typeMappings[i * 2]))
            enum getFieldType = _typeMappings[i * 2 + 1];
}

// generate an SQL statement to insert a table definition.
template createTableSql(T)
{
    string generate()
    {
        import std.traits;
        import std.typecons : Nullable;
        import sqlbuilder.uda;
        import sqlbuilder.traits;
        auto result = "CREATE TABLE `" ~ getTableName!T ~ "` (";
        foreach(field; FieldNameTuple!T)
        {
            // if it's a relationship, we will save this for later
            alias fieldType = typeof(__traits(getMember, T, field));
            static if(!is(fieldType == Relation))
            {
                string name =
                result ~= "`" ~ getColumnName!(__traits(getMember, T, field)) ~ "` ";
                enum isNullable = isInstanceOf!(Nullable, fieldType);
                static if(isNullable)
                    result ~= getFieldType!(typeof(fieldType.init.get()));
                else
                    result ~= getFieldType!fieldType;
                static if(!isNullable)
                    result ~= " NOT NULL";
                static if(hasUDA!(__traits(getMember, T, field), unique))
                    result ~= " UNIQUE";
                static if(hasUDA!(__traits(getMember, T, field), autoIncrement))
                    result ~= " AUTO INCREMENT";

                result ~= ",";
            }
        }
        // add primary key
        alias keyFields = getSymbolsByUDA!(T, primaryKey);
        static if(keyFields.length > 0)
        {
            result ~= " PRIMARY KEY (";
            static foreach(i, alias kf; keyFields)
            {
                static if(i != 0)
                    result ~= ",";
                result ~= "`" ~ getColumnName!(kf) ~ "`";
            }
            result ~= "),";
        }

        return result[0 .. $-1] ~ ")";
    }
    enum createTableSql = generate();
}

// create relations between this database and it's related ones. All tables are
// expected to exist already.
template createRelations(T)
{
}
