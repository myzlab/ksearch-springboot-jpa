package com.myzlab.k;

import com.myzlab.k.allowed.KWindowDefinitionAllowedToOver;
import com.myzlab.k.allowed.KWindowDefinitionAllowedToWindow;
import com.myzlab.k.helper.KExceptionHelper;

public class KWindowDefinitionNamedOrdered extends KWindowDefinition implements KWindowDefinitionAllowedToWindow, KWindowDefinitionAllowedToOver {
    
    private KWindowDefinitionNamedOrdered(
        final StringBuilder sb,
        final String name,
        final KColumn kColumn
    ) {
        super(sb, name);
        
        this.process(kColumn);
    }
    
    private KWindowDefinitionNamedOrdered(
        final StringBuilder sb,
        final String name,
        final KColumnOrdered kColumnOrdered
    ) {
        super(sb, name);
        
        this.process(kColumnOrdered);
    }
    
    protected static KWindowDefinitionNamedOrdered getInstance(
        final StringBuilder sb,
        final String name,
        final KColumn kColumn
    ) {
        return new KWindowDefinitionNamedOrdered(sb, name, kColumn);
    }
    
    protected static KWindowDefinitionNamedOrdered getInstance(
        final StringBuilder sb,
        final String name,
        final KColumnOrdered kColumnOrdered
    ) {
        return new KWindowDefinitionNamedOrdered(sb, name, kColumnOrdered);
    }
    
    public KWindowDefinitionNamedFrameNoStarted range() {
        return KWindowDefinitionNamedFrameNoStarted.getInstance(sb, name, "RANGE", true);
    }
    
    public KWindowDefinitionNamedFrameNoStarted rows() {
        return KWindowDefinitionNamedFrameNoStarted.getInstance(sb, name, "ROWS", true);
    }
    
    public KWindowDefinitionNamedFrameNoStarted groups() {
        return KWindowDefinitionNamedFrameNoStarted.getInstance(sb, name, "GROUPS", true);
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
