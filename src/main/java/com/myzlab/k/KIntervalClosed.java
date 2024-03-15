package com.myzlab.k;

public class KIntervalClosed extends KColumn {

    protected KIntervalClosed(
        final StringBuilder sb
    ) {
        super();
        
        this.sb.append(sb);
        
        this.process();
    }
    
    public static KIntervalClosed getInstance(
        final StringBuilder sb
    ) {
        return new KIntervalClosed(sb);
    }
    
    private void process() {
        this.sb.append("'");
    }
}
