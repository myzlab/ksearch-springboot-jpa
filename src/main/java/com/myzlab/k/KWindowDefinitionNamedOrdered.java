package com.myzlab.k;

import com.myzlab.k.allowed.KWindowDefinitionAllowedToOver;
import com.myzlab.k.allowed.KWindowDefinitionAllowedToWindow;
import com.myzlab.k.helper.KExceptionHelper;
import java.util.List;

public class KWindowDefinitionNamedOrdered extends KWindowDefinition implements KWindowDefinitionAllowedToWindow, KWindowDefinitionAllowedToOver {
    
    private KWindowDefinitionNamedOrdered(
        final StringBuilder sb,
        final String name,
        final KColumn kColumn,
        final List<Object> params
    ) {
        super(sb, name, params);
        
        this.process(kColumn);
    }
    
    private KWindowDefinitionNamedOrdered(
        final StringBuilder sb,
        final String name,
        final KColumnOrdered kColumnOrdered,
        final List<Object> params
    ) {
        super(sb, name, params);
        
        this.process(kColumnOrdered);
    }
    
    protected static KWindowDefinitionNamedOrdered getInstance(
        final StringBuilder sb,
        final String name,
        final KColumn kColumn,
        final List<Object> params
    ) {
        return new KWindowDefinitionNamedOrdered(sb, name, kColumn, params);
    }
    
    protected static KWindowDefinitionNamedOrdered getInstance(
        final StringBuilder sb,
        final String name,
        final KColumnOrdered kColumnOrdered,
        final List<Object> params
    ) {
        return new KWindowDefinitionNamedOrdered(sb, name, kColumnOrdered, params);
    }
    
    public KWindowDefinitionNamedFrameNoStarted range() {
        return KWindowDefinitionNamedFrameNoStarted.getInstance(sb, name, "RANGE", true, this.params);
    }
    
    public KWindowDefinitionNamedFrameNoStarted rows() {
        return KWindowDefinitionNamedFrameNoStarted.getInstance(sb, name, "ROWS", true, this.params);
    }
    
    public KWindowDefinitionNamedFrameNoStarted groups() {
        return KWindowDefinitionNamedFrameNoStarted.getInstance(sb, name, "GROUPS", true, this.params);
    }

    private void process(
        final KColumn kColumn
    ) {
        if (kColumn == null) {
            throw KExceptionHelper.internalServerError("The 'kColumn' param is required"); 
        }
        
//        if (!kColumn.params.isEmpty()) {
//            throw KExceptionHelper.internalServerError("Params in 'kColumn' are not allowed"); 
//        }
        
        if (this.sb.length() > 0) {
            this.sb.append(" ");
        }
        
        this.sb.append("ORDER BY ").append(kColumn.sb);
        this.params.addAll(kColumn.params);
    }
    
    private void process(
        final KColumnOrdered kColumnOrdered
    ) {
        if (kColumnOrdered == null) {
            throw KExceptionHelper.internalServerError("The 'kColumnOrdered' param is required"); 
        }
        
//        if (!kColumnOrdered.params.isEmpty()) {
//            throw KExceptionHelper.internalServerError("Params in 'kColumnOrdered' are not allowed"); 
//        }
        
        if (this.sb.length() > 0) {
            this.sb.append(" ");
        }
        
        this.sb.append("ORDER BY ").append(kColumnOrdered.sb);
        this.params.addAll(kColumnOrdered.params);
    }
    
    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public String getSql() {
        return this.sb.toString();
    }
    
    @Override
    public List<Object> getParams() {
        return this.params;
    }
}
