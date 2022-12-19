package com.myzlab.k;

public class KWindowDefinition {
    
    protected final StringBuilder sb;
    
    protected final String name;
    
    protected KWindowDefinition() {
        super();
        
        this.sb = new StringBuilder();
        this.name = null;
    }

    protected KWindowDefinition(
        final String name
    ) {
        super();
        
        this.sb = new StringBuilder();
        this.name = name;
    }
    
    protected KWindowDefinition(
        final StringBuilder sb,
        final String name
    ) {
        this(name);
        
        this.sb.append(sb);
    }
    
    protected KWindowDefinition(
        final StringBuilder sb
    ) {
        this();
        
        this.sb.append(sb);
    }
}
