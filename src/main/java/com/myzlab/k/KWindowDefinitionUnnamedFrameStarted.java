package com.myzlab.k;

import com.myzlab.k.allowed.KWindowDefinitionAllowedToOver;
import java.util.List;

public class KWindowDefinitionUnnamedFrameStarted extends KWindowDefinition implements KWindowDefinitionAllowedToOver {
    
    private final String frameStart;
    
    private KWindowDefinitionUnnamedFrameStarted(
        final StringBuilder sb,
        final String frameStart,
        final List<Object> params
    ) {
        super(sb, params);
        
        this.frameStart = frameStart;
        
        this.process();
    }
    
    protected static KWindowDefinitionUnnamedFrameStarted getInstance(
        final StringBuilder sb,
        final String frameStart,
        final List<Object> params
    ) {
        return new KWindowDefinitionUnnamedFrameStarted(sb, frameStart, params);
    }
    
    public KWindowDefinitionUnnamedFrameEnded preceding(
        int offset
    ) {
        return KWindowDefinitionUnnamedFrameEnded.getInstance(sb, frameStart, offset + " PRECEDING", this.params);
    }
    
    public KWindowDefinitionUnnamedFrameEnded currentRow() {
        return KWindowDefinitionUnnamedFrameEnded.getInstance(sb, frameStart, "CURRENT ROW", this.params);
    }
    
    public KWindowDefinitionUnnamedFrameEnded following(
        int offset
    ) {
        return KWindowDefinitionUnnamedFrameEnded.getInstance(sb, frameStart, offset + " FOLLOWING", this.params);
    }
    
    public KWindowDefinitionUnnamedFrameEnded unboundedFollowing() {
        return KWindowDefinitionUnnamedFrameEnded.getInstance(sb, frameStart, "UNBOUNDED FOLLOWING", this.params);
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
