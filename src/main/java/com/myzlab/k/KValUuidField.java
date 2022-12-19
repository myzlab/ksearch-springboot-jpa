package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToSelect;
import com.myzlab.k.allowed.KColumnAllowedToSetUpdate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class KValUuidField extends KBaseValField implements KColumnAllowedToSetUpdate, KColumnAllowedToSelect {
    
    protected KValUuidField() {
        super();
    }
    
    protected KValUuidField(
        final StringBuilder sb,
        final List<Object> params,
        final boolean closed
    ) {
        super(sb, params, closed);
    }
    
    protected KValUuidField(
        final UUID val
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
    protected KValUuidField cloneMe() {
        return new KValUuidField(this.sb, new ArrayList<>(this.params), this.closed);
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
