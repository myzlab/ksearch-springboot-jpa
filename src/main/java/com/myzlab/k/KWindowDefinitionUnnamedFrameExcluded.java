package com.myzlab.k;

import com.myzlab.k.allowed.KWindowDefinitionAllowedToOver;
import java.util.List;

public class KWindowDefinitionUnnamedFrameExcluded extends KWindowDefinition implements KWindowDefinitionAllowedToOver {
    
    private KWindowDefinitionUnnamedFrameExcluded(
        final StringBuilder sb,
        final String excluding,
        final List<Object> params
    ) {
        super(sb, params);
        
        this.process(excluding);
    }
    
    protected static KWindowDefinitionUnnamedFrameExcluded getInstance(
        final StringBuilder sb,
        final String excluding,
        final List<Object> params
    ) {
        return new KWindowDefinitionUnnamedFrameExcluded(sb, excluding, params);
    }

    private void process(
        final String excluding
    ) {
        KUtils.assertNotNull(excluding, "excluding");
        
        this.sb.append(" EXCLUDE ").append(excluding);
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
