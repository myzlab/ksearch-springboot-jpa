package com.myzlab.k;

import com.myzlab.k.allowed.KWindowDefinitionAllowedToOver;
import com.myzlab.k.helper.KExceptionHelper;

public class KWindowDefinitionUnnamedPartitioned extends KWindowDefinition implements KWindowDefinitionAllowedToOver {
    
    private KWindowDefinitionUnnamedPartitioned(
        final StringBuilder sb,
        final KColumn kColumn
    ) {
        super(sb);
        
        this.process(kColumn);
    }
    
    protected static KWindowDefinitionUnnamedPartitioned getInstance(
        final StringBuilder sb,
        final KColumn kColumn
    ) {
        return new KWindowDefinitionUnnamedPartitioned(sb, kColumn);
    }
    
    public KWindowDefinitionUnnamedOrdered orderBy(
        final KColumn kColumn
    ) {
        return KWindowDefinitionUnnamedOrdered.getInstance(sb, kColumn);
    }
    
    public KWindowDefinitionUnnamedOrdered orderBy(
        final KColumnOrdered kColumnOrdered
    ) {
        return KWindowDefinitionUnnamedOrdered.getInstance(sb, kColumnOrdered);
    }
    
    public KWindowDefinitionUnnamedFrameNoStarted range() {
        return KWindowDefinitionUnnamedFrameNoStarted.getInstance(sb, "RANGE", true);
    }
    
    public KWindowDefinitionUnnamedFrameNoStarted rows() {
        return KWindowDefinitionUnnamedFrameNoStarted.getInstance(sb, "ROWS", true);
    }
    
    private void process(
        final KColumn kColumn
    ) {
        if (kColumn == null) {
            throw KExceptionHelper.internalServerError("The 'kColumn' param is required"); 
        }
        
        if (!kColumn.params.isEmpty()) {
            throw KExceptionHelper.internalServerError("Params in 'kColumn' are not allowed"); 
        }
        
        this.sb.append("PARTITION BY ").append(kColumn.sb);
    }
    
    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public String getSql() {
        return this.sb.toString();
    }
}
