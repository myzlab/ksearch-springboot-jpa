package com.myzlab.k;

import java.util.ArrayList;
import java.util.List;

public class KCaseFilled {
    
    private final StringBuilder sb = new StringBuilder();
    private final List<Object> params = new ArrayList<>();

    protected KCaseFilled(
        final StringBuilder sb,
        final List<Object> params,
        final KBaseColumnCastable kBaseColumnCastable
    ) {
        super();
        
        this.sb.append(sb);
        this.params.addAll(params);
        
        this.process(kBaseColumnCastable);
    }
    
    public static KCaseFilled getInstance(
        final StringBuilder sb,
        final List<Object> params,
        final KBaseColumnCastable kBaseColumnCastable
    ) {
        return new KCaseFilled(sb, params, kBaseColumnCastable);
    }
    
    public static KCaseFilled getInstance(
        final StringBuilder sb,
        final List<Object> params,
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        return new KCaseFilled(sb, params, new KColumn(new StringBuilder(kRaw.content), kRaw.params, false));
    }
    
    public KCaseAliased as(
        final String alias
    ) {
        return KCaseAliased.getInstance(this.sb, this.params, alias);
    }
    
    private void process(
        final KBaseColumnCastable kBaseColumnCastable
    ) {
        KUtils.assertNotNull(kBaseColumnCastable, "kBaseColumnCastable");
        
        this.sb.append(" ELSE ").append(kBaseColumnCastable.sb);
        this.params.addAll(kBaseColumnCastable.params);
    }
}
