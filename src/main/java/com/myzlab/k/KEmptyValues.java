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
        KUtils.assertNotNullNotEmpty(value, "value");
        
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
        KUtils.assertNotNullNotEmpty(list, "list");
        
        final List<List<Object>> values = new ArrayList<>();
                
        for (final Object o : list) {
            values.add(kAppendValuesFunction.run(o));
        }
        
        final KValues kValues = KValues.getInstance(values.get(0));
        
        for (int i = 1; i < values.size(); i++) {
            kValues.append(values.get(i));
        }
        
        return kValues;
    }
}
