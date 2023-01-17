package com.myzlab.k;

import java.util.List;

public class KWindowDefinitionNamedFrameNoStarted extends KWindowDefinition {
    
    private KWindowDefinitionNamedFrameNoStarted(
        final StringBuilder sb,
        final String name,
        final String method,
        final boolean whiteSpace,
        final List<Object> params
    ) {
        super(sb, name, params);
        
        this.process(method, whiteSpace);
    }
    
    protected static KWindowDefinitionNamedFrameNoStarted getInstance(
        final StringBuilder sb,
        final String name,
        final String method,
        final boolean whiteSpace,
        final List<Object> params
    ) {
        return new KWindowDefinitionNamedFrameNoStarted(sb, name, method, whiteSpace, params);
    }
    
    public KWindowDefinitionNamedFrameStarted unboundedPreceding() {
        return KWindowDefinitionNamedFrameStarted.getInstance(sb, name, "UNBOUNDED PRECEDING", this.params);
    }
    
    public KWindowDefinitionNamedFrameStarted preceding(
        int offset
    ) {
        return KWindowDefinitionNamedFrameStarted.getInstance(sb, name, offset + " PRECEDING", this.params);
    }
    
    public KWindowDefinitionNamedFrameStarted currentRow() {
        return KWindowDefinitionNamedFrameStarted.getInstance(sb, name, "CURRENT ROW", this.params);
    }
    
    public KWindowDefinitionNamedFrameStarted following(
        int offset
    ) {
        return KWindowDefinitionNamedFrameStarted.getInstance(sb, name, offset + " FOLLOWING", this.params);
    }

    private void process(
        final String method,
        final boolean whiteSpace
    ) {
        KUtils.assertNotNull(method, "method");
        
        this.sb.append(whiteSpace ? " " : "").append(method);
    }
    
}
