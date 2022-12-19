package com.myzlab.k;

import java.util.List;

public abstract class KBaseColumnUncastable extends KBaseColumn {
    
    protected KBaseColumnUncastable() {
        super();
    }
    
    protected KBaseColumnUncastable(
        final StringBuilder sb,
        final List<Object> params,
        final boolean closed,
        final String name,
        final Class type,
        final KTable kTable
    ) {
        super(sb, params, closed, name, type, kTable);
    }
    
    protected KBaseColumnUncastable(
        final String val
    ) {
        super(val);
    }
    
    protected KBaseColumnUncastable(
        final Number val
    ) {
        super(val);
    }
}
