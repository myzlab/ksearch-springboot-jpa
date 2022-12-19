package com.myzlab.k;

import com.myzlab.k.helper.KExceptionHelper;

public abstract class KCommonTableExpression {

    protected final String name;
    protected final String[] columns;
    protected final KValues kValues;
    protected final String alias;
    protected final KQuery kQuery;
    
    protected KCommonTableExpression() {
        super();
        
        this.name = null;
        this.columns = null;
        this.kValues = null;
        this.alias = null;
        this.kQuery = null;
    }

    protected KCommonTableExpression(
        final String name
    ) {
        super();
        
        KUtils.assertNotNull(name, "name");
        
        this.name = name;
        this.columns = null;
        this.kValues = null;
        this.alias = null;
        this.kQuery = null;
    }
    
    protected KCommonTableExpression(
        final String name,
        final String... columns
    ) {
        super();
        
        KUtils.assertNotNull(name, "name");
        
        if (columns == null || columns.length == 0) {
            throw KExceptionHelper.internalServerError("The 'columns' param is required"); 
        }
        
        this.name = name;
        this.columns = columns;
        this.kValues = null;
        this.alias = null;
        this.kQuery = null;
    }
    
    protected KCommonTableExpression(
        final KValues kValues,
        final String name,
        final String... columns
    ) {
        super();
        
        KUtils.assertNotNull(kValues, "kValues");
        KUtils.assertNotNull(name, "name");
        
        if (columns == null || columns.length == 0) {
            throw KExceptionHelper.internalServerError("The 'columns' param is required"); 
        }
        
        this.name = name;
        this.columns = columns;
        this.kValues = kValues;
        this.alias = null;
        this.kQuery = null;
    }
    
    protected KCommonTableExpression(
        final KValues kValues,
        final KQuery kQuery,
        final String name,
        final String alias,
        final String... columns
    ) {
        super();
        
        if (kValues == null && kQuery == null) {
            throw KExceptionHelper.internalServerError("'KCommonTableExpression' required 'kValues' or 'kQuery' param"); 
        }
        
        KUtils.assertNotNull(name, "name");
        KUtils.assertNotNull(alias, "alias");
        
        this.name = name;
        this.columns = columns;
        this.kValues = kValues;
        this.alias = alias;
        this.kQuery = kQuery;
    }
    
    protected KCommonTableExpression(
        final KQuery kQuery,
        final String name,
        final String... columns
    ) {
        super();
        
        KUtils.assertNotNull(kQuery, "kQuery");
        KUtils.assertNotNull(name, "name");
        
        this.name = name;
        this.columns = columns;
        this.kValues = null;
        this.alias = null;
        this.kQuery = kQuery;
    }
}
