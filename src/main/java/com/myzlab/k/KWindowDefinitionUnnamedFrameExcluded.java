package com.myzlab.k;

import com.myzlab.k.allowed.KWindowDefinitionAllowedToOver;

public class KWindowDefinitionUnnamedFrameExcluded extends KWindowDefinition implements KWindowDefinitionAllowedToOver {
    
    private KWindowDefinitionUnnamedFrameExcluded(
        final StringBuilder sb,
        final String excluding
    ) {
        super(sb);
        
        this.process(excluding);
    }
    
    protected static KWindowDefinitionUnnamedFrameExcluded getInstance(
        final StringBuilder sb,
        final String excluding
    ) {
        return new KWindowDefinitionUnnamedFrameExcluded(sb, excluding);
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
}
