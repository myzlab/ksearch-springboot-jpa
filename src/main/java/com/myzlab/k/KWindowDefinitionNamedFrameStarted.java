package com.myzlab.k;

import com.myzlab.k.allowed.KWindowDefinitionAllowedToOver;
import com.myzlab.k.allowed.KWindowDefinitionAllowedToWindow;
import java.util.List;

public class KWindowDefinitionNamedFrameStarted extends KWindowDefinition implements KWindowDefinitionAllowedToWindow, KWindowDefinitionAllowedToOver {
    
    private final String frameStart;
    
    private KWindowDefinitionNamedFrameStarted(
        final StringBuilder sb,
        final String name,
        final String frameStart,
        final List<Object> params
    ) {
        super(sb, name, params);
        
        this.frameStart = frameStart;
        
        this.process();
    }
    
    protected static KWindowDefinitionNamedFrameStarted getInstance(
        final StringBuilder sb,
        final String name,
        final String frameStart,
        final List<Object> params
    ) {
        return new KWindowDefinitionNamedFrameStarted(sb, name, frameStart, params);
    }
    
    public KWindowDefinitionNamedFrameEnded preceding(
        int offset
    ) {
        return KWindowDefinitionNamedFrameEnded.getInstance(sb, name, frameStart, offset + " PRECEDING", this.params);
    }
    
    public KWindowDefinitionNamedFrameEnded currentRow() {
        return KWindowDefinitionNamedFrameEnded.getInstance(sb, name, frameStart, "CURRENT ROW", this.params);
    }
    
    public KWindowDefinitionNamedFrameEnded following(
        int offset
    ) {
        return KWindowDefinitionNamedFrameEnded.getInstance(sb, name, frameStart, offset + " FOLLOWING", this.params);
    }
    
    public KWindowDefinitionNamedFrameEnded unboundedFollowing() {
        return KWindowDefinitionNamedFrameEnded.getInstance(sb, name, frameStart, "UNBOUNDED FOLLOWING", this.params);
    }
    
    public KWindowDefinitionUnnamedFrameExcluded excludeCurrentRow() {
        return KWindowDefinitionUnnamedFrameExcluded.getInstance(sb, "CURRENT ROW", this.params);
    }
    
    public KWindowDefinitionUnnamedFrameExcluded excludeGroup() {
        return KWindowDefinitionUnnamedFrameExcluded.getInstance(sb, "GROUP", this.params);
    }
    
    public KWindowDefinitionUnnamedFrameExcluded excludeTies() {
        return KWindowDefinitionUnnamedFrameExcluded.getInstance(sb, "TIES", this.params);
    }
    
    public KWindowDefinitionUnnamedFrameExcluded excludeNoOthers() {
        return KWindowDefinitionUnnamedFrameExcluded.getInstance(sb, "NO OTHERS", this.params);
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
    
    @Override
    public List<Object> getParams() {
        return this.params;
    }
}
