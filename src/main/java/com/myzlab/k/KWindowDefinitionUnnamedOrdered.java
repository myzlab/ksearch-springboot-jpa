package com.myzlab.k;

import com.myzlab.k.allowed.KWindowDefinitionAllowedToOver;
import com.myzlab.k.helper.KExceptionHelper;
import java.util.List;

public class KWindowDefinitionUnnamedOrdered extends KWindowDefinition implements KWindowDefinitionAllowedToOver {
    
    private KWindowDefinitionUnnamedOrdered(
        final StringBuilder sb,
        final KColumn kColumn,
        final List<Object> params
    ) {
        super(sb, params);
        
        this.process(kColumn);
    }
    
    private KWindowDefinitionUnnamedOrdered(
        final StringBuilder sb,
        final KColumnOrdered kColumnOrdered,
        final List<Object> params
    ) {
        super(sb, params);
        
        this.process(kColumnOrdered);
    }
    
    protected static KWindowDefinitionUnnamedOrdered getInstance(
        final StringBuilder sb,
        final KColumn kColumn,
        final List<Object> params
    ) {
        return new KWindowDefinitionUnnamedOrdered(sb, kColumn, params);
    }
    
    protected static KWindowDefinitionUnnamedOrdered getInstance(
        final StringBuilder sb,
        final KColumnOrdered kColumnOrdered,
        final List<Object> params
    ) {
        return new KWindowDefinitionUnnamedOrdered(sb, kColumnOrdered, params);
    }
    
    public KWindowDefinitionUnnamedFrameNoStarted range() {
        return KWindowDefinitionUnnamedFrameNoStarted.getInstance(sb, "RANGE", true, this.params);
    }
    
    public KWindowDefinitionUnnamedFrameNoStarted rows() {
        return KWindowDefinitionUnnamedFrameNoStarted.getInstance(sb, "ROWS", true, this.params);
    }
    
    public KWindowDefinitionUnnamedFrameNoStarted groups() {
        return KWindowDefinitionUnnamedFrameNoStarted.getInstance(sb, "GROUPS", true, this.params);
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
