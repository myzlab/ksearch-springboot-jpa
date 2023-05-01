package com.myzlab.k;

import java.util.List;

public class KCaseEnded extends KColumn {

    protected KCaseEnded(
        final StringBuilder sb,
        final List<Object> params
    ) {
        super();
        
        this.sb.append(sb);
        this.params.addAll(params);
        
        this.process();
    }
    
    public static KCaseEnded getInstance(
        final StringBuilder sb,
        final List<Object> params
    ) {
        return new KCaseEnded(sb, params);
    }
    
    private void process() {
        this.sb.append(" END");
    }
}
