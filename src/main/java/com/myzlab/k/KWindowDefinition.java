package com.myzlab.k;

import java.util.ArrayList;
import java.util.List;

public class KWindowDefinition {
    
    protected final StringBuilder sb;
    protected final List<Object> params = new ArrayList<>();
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
        final String name,
        final List<Object> params
    ) {
        this(name);
        
        this.sb.append(sb);
        this.params.addAll(params);
    }
    
    protected KWindowDefinition(
        final StringBuilder sb,
        final List<Object> params
    ) {
        this();
        
        this.sb.append(sb);
        this.params.addAll(params);
    }
}
