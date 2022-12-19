package com.myzlab.k;

import java.util.ArrayList;
import java.util.List;

public class KValues {

    protected final List<List<Object>> values;

    private KValues(
        final List<Object> value
    ) {
        super();
        
        KUtils.assertNotNullNotEmpty(value, "value", false);
        
        this.values = new ArrayList<>();
        this.values.add(value);
    }
    
    protected static KValues getInstance(
        final List<Object> value
    ) {
        return new KValues(value);
    }
    
    public KValues append(
        final List<Object> value
    ) {
        KUtils.assertNotNullNotEmpty(value, "value", false);
        
        this.values.add(value);
        
        return this;
    }
    
    public KValues append(
        final Object... value
    ) {
        KUtils.assertNotNullNotEmpty(value, "value", false);
        
        final List<Object> v = new ArrayList<>();
        
        for (final Object o : value) {
            v.add(o);
        }
        
        this.values.add(v);
        
        return this;
    }
}
