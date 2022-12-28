package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToGroupBy;
import com.myzlab.k.allowed.KColumnAllowedToOrderBy;
import com.myzlab.k.allowed.KColumnAllowedToReturning;
import com.myzlab.k.allowed.KColumnAllowedToSelect;
import com.myzlab.k.allowed.KColumnAllowedToSetUpdate;
import com.myzlab.k.allowed.KWindowDefinitionAllowedToOver;
import java.util.ArrayList;
import java.util.List;

public class KTableColumn extends KColumn implements 
    TextMethods,
    KColumnAllowedToOrderBy,
    KColumnAllowedToGroupBy,
    KColumnAllowedToReturning,
    KColumnAllowedToSetUpdate,
    KColumnAllowedToSelect 
{
    
    private KTableColumn() {
        super();
    }
    
    public KTableColumn(
        final KTable kTable,
        final String name,
        final Class type
    ) {
        super(kTable, name, type);
    }
    
    private KTableColumn(
        final StringBuilder sb,
        final List<Object> params,
        final boolean closed,
        final String name,
        final Class type,
        final KTable kTable
    ) {
        super(sb, params, closed, name, type, kTable);
    }
    
    /**
     * 
     * @param kWindowDefinitionAllowedToOver
     * @return
     * @deprecated Over clause is not supported directly on a column
     */
    @Override
    @Deprecated
    public KColumnOvered over(
        final KWindowDefinitionAllowedToOver kWindowDefinitionAllowedToOver
    ) {
        throw new UnsupportedOperationException("Over clause is not supported directly on a column");
    }
    
    @Override
    protected KTableColumn cloneMe() {
        return new KTableColumn(new StringBuilder(this.sb), new ArrayList<>(this.params), this.closed, this.name, this.type, this.kTable);
    }
    
    @Override
    public String getSqlToOrderBy() {
        return sb.toString();
    }
    
    @Override
    public String getSqlToGroupBy() {
        return sb.toString();
    }

    @Override
    public String getSqlToReturning() {
        return sb.toString();
    }

    @Override
    public List<Object> getParams() {
        return this.params;
    }

    @Override
    public String getSqlToSet() {
        return sb.toString();
    }

    @Override
    public KBaseColumn getKBaseColumn() {
        return this;
    }
    
}
