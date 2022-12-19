package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToSelect;
import com.myzlab.k.allowed.KColumnAllowedToSetUpdate;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class KValLocalDateField extends KBaseValField implements KColumnAllowedToSetUpdate, KColumnAllowedToSelect {
    
    protected KValLocalDateField() {
        super();
    }
    
    protected KValLocalDateField(
        final StringBuilder sb,
        final List<Object> params,
        final boolean closed
    ) {
        super(sb, params, closed);
    }
    
    protected KValLocalDateField(
        final LocalDate val
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
    protected KValLocalDateField cloneMe() {
        return new KValLocalDateField(this.sb, new ArrayList<>(this.params), this.closed);
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
}
