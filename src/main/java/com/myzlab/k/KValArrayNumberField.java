package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToReturning;
import com.myzlab.k.allowed.KColumnAllowedToSelect;
import com.myzlab.k.allowed.KColumnAllowedToSetUpdate;
import java.util.ArrayList;
import java.util.List;

public class KValArrayNumberField extends KBaseValField implements KColumnAllowedToSetUpdate, KColumnAllowedToSelect, KColumnAllowedToReturning {
    
    protected KValArrayNumberField() {
        super();
    }
    
    protected KValArrayNumberField(
        final StringBuilder sb,
        final List<Object> params,
        final boolean closed
    ) {
        super(sb, params, closed);
    }
    
    protected KValArrayNumberField(
        final Number[] val
    ) {
        super(val);
    }
    
    @Override
    protected KValArrayNumberField cloneMe() {
        return new KValArrayNumberField(this.sb, new ArrayList<>(this.params), this.closed);
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
