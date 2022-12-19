package com.myzlab.k;

import com.myzlab.k.allowed.KWindowDefinitionAllowedToOver;

public class KWindowDefinitionUnnamedFrameStarted extends KWindowDefinition implements KWindowDefinitionAllowedToOver {
    
    private final String frameStart;
    
    private KWindowDefinitionUnnamedFrameStarted(
        final StringBuilder sb,
        final String frameStart
    ) {
        super(sb);
        
        this.frameStart = frameStart;
        
        this.process();
    }
    
    protected static KWindowDefinitionUnnamedFrameStarted getInstance(
        final StringBuilder sb,
        final String frameStart
    ) {
        return new KWindowDefinitionUnnamedFrameStarted(sb, frameStart);
    }
    
    public KWindowDefinitionUnnamedFrameEnded preceding(
        int offset
    ) {
        return KWindowDefinitionUnnamedFrameEnded.getInstance(sb, frameStart, offset + " PRECEDING");
    }
    
    public KWindowDefinitionUnnamedFrameEnded currentRow() {
        return KWindowDefinitionUnnamedFrameEnded.getInstance(sb, frameStart, "CURRENT ROW");
    }
    
    public KWindowDefinitionUnnamedFrameEnded following(
        int offset
    ) {
        return KWindowDefinitionUnnamedFrameEnded.getInstance(sb, frameStart, offset + " FOLLOWING");
    }
    
    public KWindowDefinitionUnnamedFrameEnded unboundedFollowing() {
        return KWindowDefinitionUnnamedFrameEnded.getInstance(sb, frameStart, "UNBOUNDED FOLLOWING");
    }
    
    private void process() {
        KUtils.assertNotNull(this.frameStart, "frameStart");
        
        this.sb.append(" ").append(this.frameStart);
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
