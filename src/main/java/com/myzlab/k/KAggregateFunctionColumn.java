package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToOrderBy;
import com.myzlab.k.allowed.KColumnAllowedToSelect;
import com.myzlab.k.allowed.KWindowDefinitionAllowedToOver;
import java.util.ArrayList;
import java.util.List;

public class KAggregateFunctionColumn extends KColumn implements 
    TextMethods,
    KColumnAllowedToOrderBy,
    KColumnAllowedToSelect
{
    
    protected KAggregateFunctionColumn() {
        super();
    }
    
    protected KAggregateFunctionColumn(
        final KTable kTable,
        final String name,
        final Class type
    ) {
        super(kTable, name, type);
    }
    
    protected KAggregateFunctionColumn(
        final StringBuilder sb,
        final List<Object> params,
        final boolean closed
    ) {
        super(sb, params, closed);
    }
    
    protected KAggregateFunctionColumn(
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
    protected KAggregateFunctionColumn cloneMe() {
        return new KAggregateFunctionColumn(new StringBuilder(this.sb), new ArrayList<>(this.params), this.closed, this.name, this.type, this.kTable);
    }
    
    @Override
    public String getSqlToOrderBy() {
        return sb.toString();
    }

    @Override
    public List<Object> getParams() {
        return this.params;
    }

    @Override
    public KBaseColumn getKBaseColumn() {
        return this;
    }
    
}
