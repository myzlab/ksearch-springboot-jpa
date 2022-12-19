package com.myzlab.k;

public class KWindowDefinitionNamedFrameNoStarted extends KWindowDefinition {
    
    private KWindowDefinitionNamedFrameNoStarted(
        final StringBuilder sb,
        final String name,
        final String method,
        final boolean whiteSpace
    ) {
        super(sb, name);
        
        this.process(method, whiteSpace);
    }
    
    protected static KWindowDefinitionNamedFrameNoStarted getInstance(
        final StringBuilder sb,
        final String name,
        final String method,
        final boolean whiteSpace
    ) {
        return new KWindowDefinitionNamedFrameNoStarted(sb, name, method, whiteSpace);
    }
    
    public KWindowDefinitionNamedFrameStarted unboundedPreceding() {
        return KWindowDefinitionNamedFrameStarted.getInstance(sb, name, "UNBOUNDED PRECEDING");
    }
    
    public KWindowDefinitionNamedFrameStarted preceding(
        int offset
    ) {
        return KWindowDefinitionNamedFrameStarted.getInstance(sb, name, offset + " PRECEDING");
    }
    
    public KWindowDefinitionNamedFrameStarted currentRow() {
        return KWindowDefinitionNamedFrameStarted.getInstance(sb, name, "CURRENT ROW");
    }
    
    public KWindowDefinitionNamedFrameStarted following(
        int offset
    ) {
        return KWindowDefinitionNamedFrameStarted.getInstance(sb, name, offset + " FOLLOWING");
    }

    private void process(
        final String method,
        final boolean whiteSpace
    ) {
        KUtils.assertNotNull(method, "method");
        
        this.sb.append(whiteSpace ? " " : "").append(method);
    }
    
}
