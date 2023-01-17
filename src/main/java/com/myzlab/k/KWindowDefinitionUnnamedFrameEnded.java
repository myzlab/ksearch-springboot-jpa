package com.myzlab.k;

import com.myzlab.k.allowed.KWindowDefinitionAllowedToOver;
import java.util.List;

public class KWindowDefinitionUnnamedFrameEnded extends KWindowDefinition implements KWindowDefinitionAllowedToOver {
    
    private KWindowDefinitionUnnamedFrameEnded(
        final StringBuilder sb,
        final String frameStart,
        final String frameEnd,
        final List<Object> params
    ) {
        super(sb, params);
        
        this.process(frameStart, frameEnd);
    }
    
    protected static KWindowDefinitionUnnamedFrameEnded getInstance(
        final StringBuilder sb,
        final String frameStart,
        final String frameEnd,
        final List<Object> params
    ) {
        return new KWindowDefinitionUnnamedFrameEnded(sb, frameStart, frameEnd, params);
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
    
    private void process(
        final String frameStart,
        final String frameEnd
    ) {
        KUtils.assertNotNull(frameStart, "frameStart");
        KUtils.assertNotNull(frameEnd, "frameEnd");
        
        this.sb.insert(this.sb.lastIndexOf(frameStart), "BETWEEN ");
        
        this.sb.append(" AND ").append(frameEnd);
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
