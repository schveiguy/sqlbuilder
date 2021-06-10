module sqlbuilder.testing;

// everything in unittest land for sqlbuilder is included here, otherwise, the
// module is blank.
/*version(unittest)
{
    // note: this is copied from mysql 
    private Variant toVariant(T)(T val)
    {
        import std.typecons : Nullable;
        static if(is(T : Nullable!U, U))
        {
            if(val.isNull)
                return Variant(null);
            return toVariant(val.get);
        }
        else static if(is(typeof(val.dbValue)))
        {
            return toVariant(val.dbValue);
        }
        else static if(is(T == enum))
        {
            import std.traits : OriginalType;
            return Variant(cast(OriginalType!T)val);
        }
        else
        {
            return Variant(val);
        }
    }

    auto param(T)(T val)
    {
        import std.range : only;
        return Parameter!Variant(only(val.toVariant));
    }
}*/
