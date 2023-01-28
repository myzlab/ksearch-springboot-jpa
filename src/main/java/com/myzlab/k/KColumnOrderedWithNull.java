package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToOrderBy;
import java.util.List;

public class KColumnOrderedWithNull extends KBaseColumn implements KColumnAllowedToOrderBy {
    
    protected KColumnOrderedWithNull() {
        super();
    }
    
    private KColumnOrderedWithNull(
        final StringBuilder sb,
        final List<Object> params,
        final boolean closed
    ) {
        super(sb, params, closed);
    }
    
    protected KColumnOrderedWithNull(
        final StringBuilder sb,
        final List<Object> params,
        final boolean closed,
        final boolean first
    ) {
        super(sb, params, closed);
        
        this.process(first);
    }

    @Override
    protected KColumnOrderedWithNull cloneMe() {
        return new KColumnOrderedWithNull(this.sb, this.params, this.closed);
    }
    
    private void process(
        final boolean first
    ) {
        this.sb.append(" NULLS ").append(first ? "FIRST" : "LAST");
    }
    
    @Override
    public String getSqlToOrderBy() {
        return this.sb.toString();
    }

    @Override
    public List<Object> getParams() {
        return this.params;
    }
}
