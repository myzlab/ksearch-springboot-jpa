package com.myzlab.k;

import com.myzlab.k.functions.KValuesFunction;
import java.util.ArrayList;
import java.util.List;

public class KEmptyValues {

    private KEmptyValues() {
        super();
    }
    
    public static KEmptyValues getInstance() {
        return new KEmptyValues();
    }
    
    public KValues append(
        final List<Object> value
    ) {
        return KValues.getInstance(value);
    }
    
    public KValues append(
        final Object... value
    ) {
        KUtils.assertNotNullNotEmpty(value, "value", false);
        
        final List<Object> v = new ArrayList<>();
        
        for (final Object o : value) {
            v.add(o);
        }
        
        return KValues.getInstance(v);
    }
    
    public KValues append(
        final List<?> list,
        final KValuesFunction kAppendValuesFunction
    ) {
        final List<Object> value = new ArrayList<>();
                
        for (final Object o : list) {
            value.add(kAppendValuesFunction.run(o));
        }
        
        return KValues.getInstance(value);
    }
}
