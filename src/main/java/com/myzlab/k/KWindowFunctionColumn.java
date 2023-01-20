package com.myzlab.k;

import com.myzlab.k.allowed.KWindowDefinitionAllowedToOver;
import java.util.ArrayList;
import java.util.List;

public class KWindowFunctionColumn extends KBaseColumn {
    
    private KWindowFunctionColumn() {
        super();
    }
    
    protected KWindowFunctionColumn(
        final StringBuilder sb,
        final boolean closed
    ) {
        super(sb, new ArrayList<>(), closed);
    }
    
    protected KWindowFunctionColumn(
        final StringBuilder sb,
        final List<Object> params,
        final boolean closed
    ) {
        super(sb, params, closed);
    }
    
    private KWindowFunctionColumn(
        final StringBuilder sb,
        final List<Object> params,
        final boolean closed,
        final String name,
        final Class type,
        final KTable kTable
    ) {
        super(sb, params, closed, name, type, kTable);
    }
    
    public KColumnOvered over(
        final KWindowDefinitionAllowedToOver kWindowDefinitionAllowedToOver
    ) {
        return new KColumnOvered(this.sb, this.params, this.closed, kWindowDefinitionAllowedToOver);
    }
    
    @Override
    protected KWindowFunctionColumn cloneMe() {
        return new KWindowFunctionColumn(new StringBuilder(this.sb), new ArrayList<>(this.params), this.closed, this.name, this.type, this.kTable);
    }
    
}
