package com.myzlab.k;

public class KPath {

    protected final String aliasSource;
    protected final String aliasTarget;
    protected final String path;

    private KPath(
        final String aliasSource,
        final String aliasTarget,
        final String path
    ) {
        this.aliasSource = aliasSource;
        this.aliasTarget = aliasTarget;
        this.path = path;
    }
    
    protected static KPath getInstance(
        final String aliasSource,
        final String aliasTarget,
        final String path
    ) {
        return new KPath(aliasSource, aliasTarget, path);
    }

    @Override
    public String toString() {
        return "KPath{" + "aliasSource=" + aliasSource + ", aliasTarget=" + aliasTarget + ", path=" + path + '}';
    }
    
}
