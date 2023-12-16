module sqlbuilder.dialect.impl;
import sqlbuilder.traits;

// these are common pieces that are used to build the dialect-specific modules.
package:
template objFieldNames(T)
{
    import std.meta : Filter;
    enum includeIt(string fieldname) = isField!(T, fieldname);
    alias objFieldNames = Filter!(includeIt, __traits(allMembers, T));
}
