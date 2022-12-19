package com.myzlab.k;

import com.myzlab.k.allowed.KWindowDefinitionAllowedToOver;

public class KWindowDefinitionUnnamedFrameEnded extends KWindowDefinition implements KWindowDefinitionAllowedToOver {
    
    private KWindowDefinitionUnnamedFrameEnded(
        final StringBuilder sb,
        final String frameStart,
        final String frameEnd
    ) {
        super(sb);
        
        this.process(frameStart, frameEnd);
    }
    
    protected static KWindowDefinitionUnnamedFrameEnded getInstance(
        final StringBuilder sb,
        final String frameStart,
        final String frameEnd
    ) {
        return new KWindowDefinitionUnnamedFrameEnded(sb, frameStart, frameEnd);
    }
    
    public KWindowDefinitionUnnamedFrameExcluded excludeCurrentRow() {
        return KWindowDefinitionUnnamedFrameExcluded.getInstance(sb, "CURRENT ROW");
    }
    
    public KWindowDefinitionUnnamedFrameExcluded excludeGroup() {
        return KWindowDefinitionUnnamedFrameExcluded.getInstance(sb, "GROUP");
    }
    
    public KWindowDefinitionUnnamedFrameExcluded excludeTies() {
        return KWindowDefinitionUnnamedFrameExcluded.getInstance(sb, "TIES");
    }
    
    public KWindowDefinitionUnnamedFrameExcluded excludeNoOthers() {
        return KWindowDefinitionUnnamedFrameExcluded.getInstance(sb, "NO OTHERS");
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
}
