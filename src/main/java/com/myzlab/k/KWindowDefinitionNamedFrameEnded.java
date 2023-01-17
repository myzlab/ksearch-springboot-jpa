package com.myzlab.k;

import com.myzlab.k.allowed.KWindowDefinitionAllowedToOver;
import com.myzlab.k.allowed.KWindowDefinitionAllowedToWindow;
import java.util.List;

public class KWindowDefinitionNamedFrameEnded extends KWindowDefinition implements KWindowDefinitionAllowedToWindow, KWindowDefinitionAllowedToOver {
    
    private KWindowDefinitionNamedFrameEnded(
        final StringBuilder sb,
        final String name,
        final String frameStart,
        final String frameEnd,
        final List<Object> params
    ) {
        super(sb, name, params);
        
        this.process(frameStart, frameEnd);
    }
    
    protected static KWindowDefinitionNamedFrameEnded getInstance(
        final StringBuilder sb,
        final String name,
        final String frameStart,
        final String frameEnd,
        final List<Object> params
    ) {
        return new KWindowDefinitionNamedFrameEnded(sb, name, frameStart, frameEnd, params);
    }
    
    public KWindowDefinitionNamedFrameExcluded excludeCurrentRow() {
        return KWindowDefinitionNamedFrameExcluded.getInstance(sb, name, "CURRENT ROW", this.params);
    }
    
    public KWindowDefinitionNamedFrameExcluded excludeGroup() {
        return KWindowDefinitionNamedFrameExcluded.getInstance(sb, name, "GROUP", this.params);
    }
    
    public KWindowDefinitionNamedFrameExcluded excludeTies() {
        return KWindowDefinitionNamedFrameExcluded.getInstance(sb, name, "TIES", this.params);
    }
    
    public KWindowDefinitionNamedFrameExcluded excludeNoOthers() {
        return KWindowDefinitionNamedFrameExcluded.getInstance(sb, name, "NO OTHERS", this.params);
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
