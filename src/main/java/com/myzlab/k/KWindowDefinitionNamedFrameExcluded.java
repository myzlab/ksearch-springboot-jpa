package com.myzlab.k;

import com.myzlab.k.allowed.KWindowDefinitionAllowedToOver;
import com.myzlab.k.allowed.KWindowDefinitionAllowedToWindow;

public class KWindowDefinitionNamedFrameExcluded extends KWindowDefinition implements KWindowDefinitionAllowedToWindow, KWindowDefinitionAllowedToOver {
    
    private KWindowDefinitionNamedFrameExcluded(
        final StringBuilder sb,
        final String name,
        final String excluding
    ) {
        super(sb, name);
        
        this.process(excluding);
    }
    
    protected static KWindowDefinitionNamedFrameExcluded getInstance(
        final StringBuilder sb,
        final String name,
        final String excluding
    ) {
        return new KWindowDefinitionNamedFrameExcluded(sb, name, excluding);
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
