package com.myzlab.k;

import java.util.List;

public class KWindowDefinitionUnnamedFrameNoStarted extends KWindowDefinition {
    
    private KWindowDefinitionUnnamedFrameNoStarted(
        final StringBuilder sb,
        final String method,
        final boolean whiteSpace,
        final List<Object> params
    ) {
        super(sb, params);
        
        this.process(method, whiteSpace);
    }
    
    protected static KWindowDefinitionUnnamedFrameNoStarted getInstance(
        final StringBuilder sb,
        final String method,
        final boolean whiteSpace,
        final List<Object> params
    ) {
        return new KWindowDefinitionUnnamedFrameNoStarted(sb, method, whiteSpace, params);
    }
    
    public KWindowDefinitionUnnamedFrameStarted unboundedPreceding() {
        return KWindowDefinitionUnnamedFrameStarted.getInstance(sb, "UNBOUNDED PRECEDING", this.params);
    }
    
    public KWindowDefinitionUnnamedFrameStarted preceding(
        int offset
    ) {
        return KWindowDefinitionUnnamedFrameStarted.getInstance(sb, offset + " PRECEDING", this.params);
    }
    
    public KWindowDefinitionUnnamedFrameStarted currentRow() {
        return KWindowDefinitionUnnamedFrameStarted.getInstance(sb, "CURRENT ROW", this.params);
    }
    
    public KWindowDefinitionUnnamedFrameStarted following(
        int offset
    ) {
        return KWindowDefinitionUnnamedFrameStarted.getInstance(sb, offset + " FOLLOWING", this.params);
    }

    private void process(
        final String method,
        final boolean whiteSpace
    ) {
        KUtils.assertNotNull(method, "method");
        
        this.sb.append(whiteSpace ? " " : "").append(method);
    }
    
}
