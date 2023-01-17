package com.myzlab.k;

import com.myzlab.k.allowed.KWindowDefinitionAllowedToOver;
import com.myzlab.k.helper.KExceptionHelper;
import java.util.ArrayList;
import java.util.List;

public class KWindowDefinitionUnnamedPartitioned extends KWindowDefinition implements KWindowDefinitionAllowedToOver {
    
    private KWindowDefinitionUnnamedPartitioned(
        final StringBuilder sb,
        final KColumn kColumn
    ) {
        super(sb, new ArrayList<>());
        
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
        return KWindowDefinitionUnnamedOrdered.getInstance(sb, kColumn, this.params);
    }
    
    public KWindowDefinitionUnnamedOrdered orderBy(
        final KColumnOrdered kColumnOrdered
    ) {
        return KWindowDefinitionUnnamedOrdered.getInstance(sb, kColumnOrdered, this.params);
    }
    
    public KWindowDefinitionUnnamedFrameNoStarted range() {
        return KWindowDefinitionUnnamedFrameNoStarted.getInstance(sb, "RANGE", true, this.params);
    }
    
    public KWindowDefinitionUnnamedFrameNoStarted rows() {
        return KWindowDefinitionUnnamedFrameNoStarted.getInstance(sb, "ROWS", true, this.params);
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
        
        this.sb.append("PARTITION BY ").append(kColumn.sb);
        this.params.addAll(kColumn.params);
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
