package com.myzlab.k;

import com.myzlab.k.helper.KExceptionHelper;

public abstract class KCommonTableExpression {

    protected final String name;
    protected final String[] columns;
    protected final KValues kValues;
    protected final String alias;
    protected final KGenericQuery kGenericQuery;
    
    protected KCommonTableExpression() {
        super();
        
        this.name = null;
        this.columns = null;
        this.kValues = null;
        this.alias = null;
        this.kGenericQuery = null;
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
        this.kGenericQuery = null;
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
        this.kGenericQuery = null;
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
        this.kGenericQuery = null;
    }
    
    protected KCommonTableExpression(
        final KValues kValues,
        final KGenericQuery kGenericQuery,
        final String name,
        final String alias,
        final String... columns
    ) {
        super();
        
        if (kValues == null && kGenericQuery == null) {
            throw KExceptionHelper.internalServerError("'KCommonTableExpression' required 'kValues' or 'kGenericQuery' param"); 
        }
        
        KUtils.assertNotNull(name, "name");
        KUtils.assertNotNull(alias, "alias");
        
        this.name = name;
        this.columns = columns;
        this.kValues = kValues;
        this.alias = alias;
        this.kGenericQuery = kGenericQuery;
    }
    
    protected KCommonTableExpression(
        final KGenericQuery kGenericQuery,
        final String name,
        final String... columns
    ) {
        super();
        
        KUtils.assertNotNull(kGenericQuery, "kGenericQuery");
        KUtils.assertNotNull(name, "name");
        
        this.name = name;
        this.columns = columns;
        this.kValues = null;
        this.alias = null;
        this.kGenericQuery = kGenericQuery;
    }
}
