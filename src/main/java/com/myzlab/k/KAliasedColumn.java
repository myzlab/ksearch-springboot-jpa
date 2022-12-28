package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToReturning;
import com.myzlab.k.allowed.KColumnAllowedToSelect;
import com.myzlab.k.helper.KExceptionHelper;
import java.util.ArrayList;
import java.util.List;

public class KAliasedColumn extends KBaseColumnUncastable implements KColumnAllowedToReturning, KColumnAllowedToSelect {
    
    protected String alias;
    
    private KAliasedColumn() {
        super();
    }
    
    protected KAliasedColumn(
        final StringBuilder sb,
        final String alias,
        final List<Object> params,
        final boolean closed,
        final String name,
        final Class type,
        final KTable kTable
    ) {
        super(sb, params, closed, name, type, kTable);
        
        this.alias = alias;
    }
    
    protected KAliasedColumn(
        final KBaseColumnCastable kBaseColumnCastable,
        final String alias
    ) {
        super(kBaseColumnCastable.sb, kBaseColumnCastable.params, false, kBaseColumnCastable.name, kBaseColumnCastable.type, kBaseColumnCastable.kTable);
        
        if (alias == null) {
            throw KExceptionHelper.internalServerError("The 'alias' param is required");
        }
        
        this.alias = alias;
        
        this.process(alias);
    }
    
    protected KAliasedColumn(
        final KColumnOvered kColumnOvered,
        final String alias
    ) {
        super(kColumnOvered.sb, kColumnOvered.params, false, kColumnOvered.name, kColumnOvered.type, kColumnOvered.kTable);
        
        if (alias == null) {
            throw KExceptionHelper.internalServerError("The 'alias' param is required");
        }
        
        this.alias = alias;
        
        this.process(alias);
    }
    
    protected KAliasedColumn(
        final StringBuilder sb,
        final String alias,
        final List<Object> params,
        final boolean closed
    ) {
        super(sb, params, closed, null, null, null);
        
        if (alias == null) {
            throw KExceptionHelper.internalServerError("The 'alias' param is required");
        }
        
        this.alias = alias;
        
        this.process(alias);
    }
    
    private void process(
        final String alias
    ) {
        this.sb.append(" AS \"").append(alias).append("\"");
    }
    
    @Override
    protected KAliasedColumn cloneMe() {
        return new KAliasedColumn(this.sb, this.alias, new ArrayList<>(this.params), this.closed, this.name, this.type, this.kTable);
    }

    @Override
    public String getSqlToReturning() {
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
}
