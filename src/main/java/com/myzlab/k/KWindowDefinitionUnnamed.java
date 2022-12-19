package com.myzlab.k;

public class KWindowDefinitionUnnamed extends KWindowDefinition {

    private KWindowDefinitionUnnamed() {
        super();
    }
    
    protected static KWindowDefinitionUnnamed getInstance() {
        return new KWindowDefinitionUnnamed();
    }
    
    public KWindowDefinitionNamed name(
        final String name
    ) {
        return KWindowDefinitionNamed.getInstance(sb, name);
    }
    
    public KWindowDefinitionUnnamedPartitioned partitionBy(
        final KColumn kColumn
    ) {
        return KWindowDefinitionUnnamedPartitioned.getInstance(sb, kColumn);
    }
    
    public KWindowDefinitionUnnamedOrdered orderBy(
        final KColumn kColumn
    ) {
        return KWindowDefinitionUnnamedOrdered.getInstance(sb, kColumn);
    }
    
    public KWindowDefinitionUnnamedOrdered orderBy(
        final KColumnOrdered kColumnOrdered
    ) {
        return KWindowDefinitionUnnamedOrdered.getInstance(sb, kColumnOrdered);
    }
    
    public KWindowDefinitionUnnamedFrameNoStarted range() {
        return KWindowDefinitionUnnamedFrameNoStarted.getInstance(sb, "RANGE", false);
    }
    
    public KWindowDefinitionUnnamedFrameNoStarted rows() {
        return KWindowDefinitionUnnamedFrameNoStarted.getInstance(sb, "ROWS", false);
    }
}
