package com.myzlab.k;

public class KWindowDefinitionUnnamedFrameNoStarted extends KWindowDefinition {
    
    private KWindowDefinitionUnnamedFrameNoStarted(
        final StringBuilder sb,
        final String method,
        final boolean whiteSpace
    ) {
        super(sb);
        
        this.process(method, whiteSpace);
    }
    
    protected static KWindowDefinitionUnnamedFrameNoStarted getInstance(
        final StringBuilder sb,
        final String method,
        final boolean whiteSpace
    ) {
        return new KWindowDefinitionUnnamedFrameNoStarted(sb, method, whiteSpace);
    }
    
    public KWindowDefinitionUnnamedFrameStarted unboundedPreceding() {
        return KWindowDefinitionUnnamedFrameStarted.getInstance(sb, "UNBOUNDED PRECEDING");
    }
    
    public KWindowDefinitionUnnamedFrameStarted preceding(
        int offset
    ) {
        return KWindowDefinitionUnnamedFrameStarted.getInstance(sb, offset + " PRECEDING");
    }
    
    public KWindowDefinitionUnnamedFrameStarted currentRow() {
        return KWindowDefinitionUnnamedFrameStarted.getInstance(sb, "CURRENT ROW");
    }
    
    public KWindowDefinitionUnnamedFrameStarted following(
        int offset
    ) {
        return KWindowDefinitionUnnamedFrameStarted.getInstance(sb, offset + " FOLLOWING");
    }

    private void process(
        final String method,
        final boolean whiteSpace
    ) {
        KUtils.assertNotNull(method, "method");
        
        this.sb.append(whiteSpace ? " " : "").append(method);
    }
    
}
