package com.myzlab.k;

import com.myzlab.k.allowed.KWindowDefinitionAllowedToOver;
import com.myzlab.k.allowed.KWindowDefinitionAllowedToWindow;

public class KWindowDefinitionNamed extends KWindowDefinition implements KWindowDefinitionAllowedToWindow, KWindowDefinitionAllowedToOver {
    
    private KWindowDefinitionNamed() {
        super();
    }

    private KWindowDefinitionNamed(
        final String name
    ) {
        super(name);
    }
    
    private KWindowDefinitionNamed(
        final StringBuilder sb,
        final String name
    ) {
        super(sb, name);
    }
    
    protected static KWindowDefinitionNamed getInstance(
        final StringBuilder sb,
        final String name
    ) {
        return new KWindowDefinitionNamed(sb, name);
    }
    
    protected static KWindowDefinitionNamed getInstance(
        final String name
    ) {
        return new KWindowDefinitionNamed(name);
    }
    
    public KWindowDefinitionNamedPartitioned partitionBy(
        final KColumn kColumn
    ) {
        return KWindowDefinitionNamedPartitioned.getInstance(sb, name, kColumn);
    }
    
    public KWindowDefinitionNamedOrdered orderBy(
        final KColumn kColumn
    ) {
        return KWindowDefinitionNamedOrdered.getInstance(sb, name, kColumn);
    }
    
    public KWindowDefinitionNamedOrdered orderBy(
        final KColumnOrdered kColumnOrdered
    ) {
        return KWindowDefinitionNamedOrdered.getInstance(sb, name, kColumnOrdered);
    }
    
    public KWindowDefinitionNamedFrameNoStarted range() {
        return KWindowDefinitionNamedFrameNoStarted.getInstance(sb, name, "RANGE", false);
    }
    
    public KWindowDefinitionNamedFrameNoStarted rows() {
        return KWindowDefinitionNamedFrameNoStarted.getInstance(sb, name, "ROWS", false);
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
