package com.myzlab.k;

import com.myzlab.k.allowed.KWindowDefinitionAllowedToOver;
import com.myzlab.k.helper.KExceptionHelper;

public class KWindowDefinitionUnnamedOrdered extends KWindowDefinition implements KWindowDefinitionAllowedToOver {
    
    private KWindowDefinitionUnnamedOrdered(
        final StringBuilder sb,
        final KColumn kColumn
    ) {
        super(sb);
        
        this.process(kColumn);
    }
    
    private KWindowDefinitionUnnamedOrdered(
        final StringBuilder sb,
        final KColumnOrdered kColumnOrdered
    ) {
        super(sb);
        
        this.process(kColumnOrdered);
    }
    
    protected static KWindowDefinitionUnnamedOrdered getInstance(
        final StringBuilder sb,
        final KColumn kColumn
    ) {
        return new KWindowDefinitionUnnamedOrdered(sb, kColumn);
    }
    
    protected static KWindowDefinitionUnnamedOrdered getInstance(
        final StringBuilder sb,
        final KColumnOrdered kColumnOrdered
    ) {
        return new KWindowDefinitionUnnamedOrdered(sb, kColumnOrdered);
    }
    
    public KWindowDefinitionUnnamedFrameNoStarted range() {
        return KWindowDefinitionUnnamedFrameNoStarted.getInstance(sb, "RANGE", true);
    }
    
    public KWindowDefinitionUnnamedFrameNoStarted rows() {
        return KWindowDefinitionUnnamedFrameNoStarted.getInstance(sb, "ROWS", true);
    }
    
    public KWindowDefinitionUnnamedFrameNoStarted groups() {
        return KWindowDefinitionUnnamedFrameNoStarted.getInstance(sb, "GROUPS", true);
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
        
        if (this.sb.length() > 0) {
            this.sb.append(" ");
        }
        
        this.sb.append("ORDER BY ").append(kColumn.sb);
    }
    
    private void process(
        final KColumnOrdered kColumnOrdered
    ) {
        if (kColumnOrdered == null) {
            throw KExceptionHelper.internalServerError("The 'kColumnOrdered' param is required"); 
        }
        
        if (!kColumnOrdered.params.isEmpty()) {
            throw KExceptionHelper.internalServerError("Params in 'kColumnOrdered' are not allowed"); 
        }
        
        if (this.sb.length() > 0) {
            this.sb.append(" ");
        }
        
        this.sb.append("ORDER BY ").append(kColumnOrdered.sb);
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
