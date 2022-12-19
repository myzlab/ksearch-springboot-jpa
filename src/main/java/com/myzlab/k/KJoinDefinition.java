package com.myzlab.k;

import com.myzlab.k.helper.KExceptionHelper;
import java.util.List;

public class KJoinDefinition {

    protected final List<Object> params;
    protected final String table;
    protected final KCondition kCondition;
    protected final KEdge kEdge;
    
    private KJoinDefinition() {
        super();
        
        this.params = null;
        this.table = null;
        this.kCondition = null;
        this.kEdge = null;
    }
    
    private KJoinDefinition(
        final String table,
        final KCondition kCondition
    ) {
        super();
        
        this.params = null;
        this.table = table;
        this.kCondition = kCondition;
        this.kEdge = null;
    }
    
    private KJoinDefinition(
        final String table,
        final KCondition kCondition,
        final KEdge kEdge
    ) {
        super();
        
        this.params = null;
        this.table = table;
        this.kCondition = kCondition;
        this.kEdge = kEdge;
    }
    
    private KJoinDefinition(
        final List<Object> params,
        final String table,
        final KCondition kCondition
    ) {
        super();
        
        this.params = params;
        this.table = table;
        this.kCondition = kCondition;
        this.kEdge = null;
    }
    
    private KJoinDefinition(
        final List<Object> params,
        final String table,
        final KCondition kCondition,
        final KEdge kEdge
    ) {
        super();
        
        this.params = params;
        this.table = table;
        this.kCondition = kCondition;
        this.kEdge = kEdge;
    }
    
    protected static KJoinDefinition getInstance(
        final String table,
        final KCondition kCondition
    ) {
        KUtils.assertNotNull(kCondition, "kCondition");
        
        if (kCondition.emptyCondition) {
            throw KExceptionHelper.internalServerError("'EmptyCondition' is not supported in 'JOIN' clause");
        }
        
        return new KJoinDefinition(table, kCondition);
    }
    
    protected static KJoinDefinition getInstance(
        final String table,
        final KCondition kCondition,
        final KEdge kEdge
    ) {
        KUtils.assertNotNull(kCondition, "kCondition");
        
        if (kCondition.emptyCondition) {
            throw KExceptionHelper.internalServerError("'EmptyCondition' is not supported in 'JOIN' clause");
        }
        
        return new KJoinDefinition(table, kCondition, kEdge);
    }
    
    protected static KJoinDefinition getInstance(
        final List<Object> params,
        final String table,
        final KCondition kCondition
    ) {
        KUtils.assertNotNull(kCondition, "kCondition");
        
        if (kCondition.emptyCondition) {
            throw KExceptionHelper.internalServerError("'EmptyCondition' is not supported in 'JOIN' clause");
        }
        
        return new KJoinDefinition(params, table, kCondition);
    }
    
    protected static KJoinDefinition getInstance(
        final List<Object> params,
        final String table,
        final KCondition kCondition,
        final KEdge kEdge
    ) {
        KUtils.assertNotNull(kCondition, "kCondition");
        KUtils.assertNotNull(kEdge, "kEdge");
        
        if (kCondition.emptyCondition) {
            throw KExceptionHelper.internalServerError("'EmptyCondition' is not supported in 'JOIN' clause");
        }
        
        return new KJoinDefinition(params, table, kCondition, kEdge);
    }
}
