package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToReturning;
import com.myzlab.k.allowed.KColumnAllowedToSelect;
import com.myzlab.k.allowed.KColumnAllowedToSetUpdate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class KValLocalDateTimeField extends KBaseValField implements KColumnAllowedToSetUpdate, KColumnAllowedToSelect, KColumnAllowedToReturning {
    
    protected KValLocalDateTimeField() {
        super();
    }
    
    protected KValLocalDateTimeField(
        final StringBuilder sb,
        final List<Object> params,
        final boolean closed
    ) {
        super(sb, params, closed);
    }
    
    protected KValLocalDateTimeField(
        final LocalDateTime val
    ) {
        super(val);
    }
    
    public KColumnOrdered asc() {
        return new KColumnOrdered(sb, params, closed, 1);
    }
    
    public KColumnOrdered desc() {
        return new KColumnOrdered(sb, params, closed, -1);
    }
    
    @Override
    protected KValLocalDateTimeField cloneMe() {
        return new KValLocalDateTimeField(this.sb, new ArrayList<>(this.params), this.closed);
    }

    @Override
    public String getSqlToSet() {
        return this.sb.toString();
    }

    @Override
    public List<Object> getParams() {
        return this.params;
    }
    
    @Override
    public KBaseColumn getKBaseColumn() {
        return this;
    }
    
    @Override
    public String getSqlToReturning() {
        return sb.toString();
    }
}
