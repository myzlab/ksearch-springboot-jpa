package com.myzlab.k;

import lombok.Data;

@Data
public class KEdge {

    protected final Class<? extends KRow> source;
    protected final String aliasSource;
    protected final Class<? extends KRow> target;
    protected final String aliasTarget;
    protected final String path;

    private KEdge(
        final Class<? extends KRow> source,
        final String aliasSource,
        final Class<? extends KRow> target,
        final String aliasTarget,
        final String path
    ) {
        this.source = source;
        this.aliasSource = aliasSource;
        this.target = target;
        this.aliasTarget = aliasTarget;
        this.path = path;
    }
    
    public static KEdge getInstance(
        final Class<? extends KRow> source,
        final String aliasSource,
        final Class<? extends KRow> target,
        final String aliasTarget,
        final String path
    ) {
        return new KEdge(source, aliasSource, target, aliasTarget, path);
    }
}
