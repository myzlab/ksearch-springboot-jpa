package com.myzlab.k;

import java.util.ArrayList;
import java.util.List;

public class KCasePartial {
    
    private final StringBuilder sb = new StringBuilder();
    private final List<Object> params = new ArrayList<>();
    
    protected KCasePartial(
        final StringBuilder sb,
        final List<Object> params,
        final KBaseColumnCastable kBaseColumnCastable
    ) {
        super();
        
        this.sb.append(sb);
        this.params.addAll(params);
        
        this.process(kBaseColumnCastable);
    }
    
    public static KCasePartial getInstance(
        final StringBuilder sb,
        final List<Object> params,
        final KBaseColumnCastable kBaseColumnCastable
    ) {
        return new KCasePartial(sb, params, kBaseColumnCastable);
    }
    
    public static KCasePartial getInstance(
        final StringBuilder sb,
        final List<Object> params,
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        return new KCasePartial(sb, params, new KColumn(new StringBuilder(kRaw.content), kRaw.params, false));
    }
    
    public KCaseWhen when(
        final KCondition kCondition
    ) {
        return KCaseWhen.getInstance(this.sb, this.params, kCondition);
    }
    
    public KCaseFilled elseResult(
        final KBaseColumnCastable kBaseColumnCastable
    ) {
        return KCaseFilled.getInstance(this.sb, this.params, kBaseColumnCastable);
    }
    
    public KCaseFilled elseResult(
        final KRaw kRaw
    ) {
        return KCaseFilled.getInstance(this.sb, this.params, kRaw);
    }
    
    public KCaseEnded end() {
        return KCaseEnded.getInstance(this.sb, this.params);
    }
    
    private void process(
        final KBaseColumnCastable kBaseColumnCastable
    ) {
        KUtils.assertNotNull(kBaseColumnCastable, "kBaseColumnCastable");
        
        this.sb.append(" THEN ").append(kBaseColumnCastable.sb);
        this.params.addAll(kBaseColumnCastable.params);
    }
}
