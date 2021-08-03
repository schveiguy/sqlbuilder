/**
 * Copyright: 2021 Steven Schveighoffer
 * License: Boost-1.0, see LICENSE.md
 */
module sqlbuilder.util;

package struct BitStack
{
    private enum bitsInSizeT = size_t.sizeof * 8;
    private enum topBit = size_t(1) << (bitsInSizeT - 1);
    private union State {
        size_t[2] staticBits;
        size_t[] bitarr;
    }
    private State state;
    private size_t top;
    
    @disable this(this);

@nogc @trusted nothrow /*pure*/ :

    @property size_t length() const { return top & ~topBit; }

    private @property bool isAllocated() const { return (top & topBit) ? true : false; }

    @property private inout(size_t)[] bitarr() inout
    {
        return isAllocated ? state.bitarr : state.staticBits[];
    }

    ~this()
    {
        import core.stdc.stdlib : free;
        if(isAllocated)
        {
            free(state.bitarr.ptr);
            state.bitarr = null;
            top = 0;
        }
    }

    void clear() {
        top &= topBit;
    }

    void push(bool val)
    {
        import core.stdc.stdlib : realloc, malloc;
        import core.bitop : bts, btr;
        size_t lenNeeded = (length + bitsInSizeT) / bitsInSizeT;
        if(bitarr.length < lenNeeded)
        {
            if(isAllocated)
            {
                state.bitarr = (cast(size_t *)realloc(state.bitarr.ptr, lenNeeded * size_t.sizeof))[0 .. lenNeeded];
            }
            else
            {
                auto arr = (cast(size_t *)malloc(lenNeeded * size_t.sizeof))[0 .. lenNeeded];
                arr.ptr[0 .. state.staticBits.length] = state.staticBits[];
                top |= topBit;// flag as allocated
                state.bitarr = arr;
                assert(isAllocated);
            }
        }
        if(val)
            bts(bitarr.ptr, top);
        else
            btr(bitarr.ptr, top);
        ++top;
    }

    bool peek() const
    {
        assert(length > 0, "Cannot peek at top of an empty stack");
        import core.bitop : bt;
        return bt(bitarr.ptr, length - 1) ? true : false;
    }

    bool pop()
    {
        assert(length > 0, "Cannot pop top of an empty stack");
        import core.bitop : bt;
        --top;
        return bt(bitarr.ptr, length) ? true : false;
    }
}

unittest
{
    // test bitstack
    BitStack bs;
    bs.push(true);
    bs.push(false);
    bs.push(true);
    bs.push(false);
    assert(bs.length == 4);
    assert(!bs.peek);
    assert(!bs.pop);
    assert(bs.length == 3);
    assert(bs.pop);
    assert(!bs.pop);
    assert(bs.peek);
    assert(bs.length == 1);
    assert(bs.pop);
    assert(bs.length == 0);
}

