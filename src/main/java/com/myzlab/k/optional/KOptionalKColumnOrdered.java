package com.myzlab.k.optional;

import com.myzlab.k.KColumnOrdered;
import com.myzlab.k.allowed.KColumnAllowedToOrderBy;
import java.util.List;

public class KOptionalKColumnOrdered implements KColumnAllowedToOrderBy {

    private final KColumnOrdered kColumnOrdered;
    
    private KOptionalKColumnOrdered() {
        super();
        
        this.kColumnOrdered = null;
    }
    
    private KOptionalKColumnOrdered(
        final KColumnOrdered kColumnOrdered
    ) {
        super();
        
        this.kColumnOrdered = kColumnOrdered;
    }
    
    public static KOptionalKColumnOrdered getNullInstance() {
        return new KOptionalKColumnOrdered();
    }
    
    public static KOptionalKColumnOrdered getInstance(
        final KColumnOrdered kColumnOrdered
    ) {
        return new KOptionalKColumnOrdered(kColumnOrdered);
    }
    
    public KColumnOrdered get() {
        return kColumnOrdered;
    }
    
    public boolean isPresent() {
        return kColumnOrdered != null;
    }

    @Override
    public String getSqlToOrderBy() {
        if (!isPresent()) {
            return null;
        }
        
        return this.kColumnOrdered.getSqlToOrderBy();
    }

    @Override
    public List<Object> getParams() {
        if (!isPresent()) {
            return null;
        }
        
        return this.kColumnOrdered.getParams();
    }
}
