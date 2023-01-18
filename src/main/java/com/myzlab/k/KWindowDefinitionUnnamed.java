package com.myzlab.k;

import com.myzlab.k.allowed.KWindowDefinitionAllowedToOver;
import java.util.List;

public class KWindowDefinitionUnnamed extends KWindowDefinition implements KWindowDefinitionAllowedToOver {

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
        return KWindowDefinitionUnnamedOrdered.getInstance(sb, kColumn, this.params);
    }
    
    public KWindowDefinitionUnnamedOrdered orderBy(
        final KColumnOrdered kColumnOrdered
    ) {
        return KWindowDefinitionUnnamedOrdered.getInstance(sb, kColumnOrdered, this.params);
    }
    
    public KWindowDefinitionUnnamedFrameNoStarted range() {
        return KWindowDefinitionUnnamedFrameNoStarted.getInstance(sb, "RANGE", false, this.params);
    }
    
    public KWindowDefinitionUnnamedFrameNoStarted rows() {
        return KWindowDefinitionUnnamedFrameNoStarted.getInstance(sb, "ROWS", false, this.params);
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
