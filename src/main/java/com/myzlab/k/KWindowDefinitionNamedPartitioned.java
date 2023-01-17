package com.myzlab.k;

import com.myzlab.k.allowed.KWindowDefinitionAllowedToOver;
import com.myzlab.k.allowed.KWindowDefinitionAllowedToWindow;
import com.myzlab.k.helper.KExceptionHelper;
import java.util.ArrayList;
import java.util.List;

public class KWindowDefinitionNamedPartitioned extends KWindowDefinition implements KWindowDefinitionAllowedToWindow, KWindowDefinitionAllowedToOver {
    
    private KWindowDefinitionNamedPartitioned(
        final StringBuilder sb,
        final String name,
        final KColumn kColumn
    ) {
        super(sb, name, new ArrayList<>());
        
        this.process(kColumn);
    }
    
    protected static KWindowDefinitionNamedPartitioned getInstance(
        final StringBuilder sb,
        final String name,
        final KColumn kColumn
    ) {
        return new KWindowDefinitionNamedPartitioned(sb, name, kColumn);
    }
    
    public KWindowDefinitionNamedOrdered orderBy(
        final KColumn kColumn
    ) {
        return KWindowDefinitionNamedOrdered.getInstance(sb, name, kColumn, this.params);
    }
    
    public KWindowDefinitionNamedOrdered orderBy(
        final KColumnOrdered kColumnOrdered
    ) {
        return KWindowDefinitionNamedOrdered.getInstance(sb, name, kColumnOrdered, this.params);
    }
    
    public KWindowDefinitionNamedFrameNoStarted range() {
        return KWindowDefinitionNamedFrameNoStarted.getInstance(sb, name, "RANGE", true, this.params);
    }
    
    public KWindowDefinitionNamedFrameNoStarted rows() {
        return KWindowDefinitionNamedFrameNoStarted.getInstance(sb, name, "ROWS", true, this.params);
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
