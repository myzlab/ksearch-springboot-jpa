package com.myzlab.k;

import com.myzlab.k.allowed.KWindowDefinitionAllowedToOver;
import com.myzlab.k.allowed.KWindowDefinitionAllowedToWindow;

public class KWindowDefinitionNamedFrameStarted extends KWindowDefinition implements KWindowDefinitionAllowedToWindow, KWindowDefinitionAllowedToOver {
    
    private final String frameStart;
    
    private KWindowDefinitionNamedFrameStarted(
        final StringBuilder sb,
        final String name,
        final String frameStart
    ) {
        super(sb, name);
        
        this.frameStart = frameStart;
        
        this.process();
    }
    
    protected static KWindowDefinitionNamedFrameStarted getInstance(
        final StringBuilder sb,
        final String name,
        final String frameStart
    ) {
        return new KWindowDefinitionNamedFrameStarted(sb, name, frameStart);
    }
    
    public KWindowDefinitionNamedFrameEnded preceding(
        int offset
    ) {
        return KWindowDefinitionNamedFrameEnded.getInstance(sb, name, frameStart, offset + " PRECEDING");
    }
    
    public KWindowDefinitionNamedFrameEnded currentRow() {
        return KWindowDefinitionNamedFrameEnded.getInstance(sb, name, frameStart, "CURRENT ROW");
    }
    
    public KWindowDefinitionNamedFrameEnded following(
        int offset
    ) {
        return KWindowDefinitionNamedFrameEnded.getInstance(sb, name, frameStart, offset + " FOLLOWING");
    }
    
    public KWindowDefinitionNamedFrameEnded unboundedFollowing() {
        return KWindowDefinitionNamedFrameEnded.getInstance(sb, name, frameStart, "UNBOUNDED FOLLOWING");
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
