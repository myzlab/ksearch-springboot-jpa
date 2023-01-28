package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToOrderBy;
import java.util.List;

public class KColumnOrdered extends KBaseColumn implements KColumnAllowedToOrderBy {
    
    protected KColumnOrdered() {
        super();
    }
    
    private KColumnOrdered(
        final StringBuilder sb,
        final List<Object> params,
        final boolean closed
    ) {
        super(sb, params, closed);
    }
    
    protected KColumnOrdered(
        final StringBuilder sb,
        final List<Object> params,
        final boolean closed,
        final int orientation
    ) {
        super(sb, params, closed);
        
        this.process(orientation);
    }
    
    public KColumnOrderedWithNull nullsFirst() {
        return new KColumnOrderedWithNull(this.sb, this.params, this.closed, true);
    }
    
    public KColumnOrderedWithNull nullsLast() {
        return new KColumnOrderedWithNull(this.sb, this.params, this.closed, false);
    }

    @Override
    protected KColumnOrdered cloneMe() {
        return new KColumnOrdered(this.sb, this.params, this.closed);
    }
    
    private void process(
        final int orientation
    ) {
        this.sb.append(" ").append(orientation > 0 ? "ASC" : "DESC");
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
