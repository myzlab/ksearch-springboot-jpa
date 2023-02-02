package com.myzlab.k.optional;

import com.myzlab.k.KBaseColumn;
import com.myzlab.k.KColumn;
import com.myzlab.k.allowed.KColumnAllowedToSelect;

public class KOptionalKColumn implements KColumnAllowedToSelect {

    private final KColumn kColumn;
    
    private KOptionalKColumn() {
        super();
        
        this.kColumn = null;
    }
    
    private KOptionalKColumn(
        final KColumn kColumn
    ) {
        super();
        
        this.kColumn = kColumn;
    }
    
    public static KOptionalKColumn getInstance(
        final KColumn kColumn
    ) {
        return new KOptionalKColumn(kColumn);
    }
    
    public KColumn get() {
        return kColumn;
    }
    
    public boolean isPresent() {
        return kColumn != null;
    }
    
    @Override
    public KBaseColumn getKBaseColumn() {
        if (isPresent()) {
            return kColumn.getKBaseColumn();
        }
        
        return null;
    }
}
