package com.myzlab.k.optional;

import com.myzlab.k.KAliasedColumn;
import com.myzlab.k.KBaseColumn;
import com.myzlab.k.allowed.KColumnAllowedToSelect;

public class KOptionalKAliasedColumn implements KColumnAllowedToSelect {

    private final KAliasedColumn kAliasedColumn;
    
    private KOptionalKAliasedColumn() {
        super();
        
        this.kAliasedColumn = null;
    }
    
    private KOptionalKAliasedColumn(
        final KAliasedColumn kAliasedColumn
    ) {
        super();
        
        this.kAliasedColumn = kAliasedColumn;
    }
    
    public static KOptionalKAliasedColumn getInstance(
        final KAliasedColumn kAliasedColumn
    ) {
        return new KOptionalKAliasedColumn(kAliasedColumn);
    }
    
    public KAliasedColumn get() {
        return kAliasedColumn;
    }
    
    public boolean isPresent() {
        return kAliasedColumn != null;
    }
    
    @Override
    public KBaseColumn getKBaseColumn() {
        if (isPresent()) {
            return kAliasedColumn.getKBaseColumn();
        }
        
        return null;
    }
}
