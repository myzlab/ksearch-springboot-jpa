package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToOrderBy;
import com.myzlab.k.allowed.KColumnAllowedToSelect;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class KArrayAggColumn extends KAggregateFunctionColumn implements
    TextMethods,
    KColumnAllowedToOrderBy,
    KColumnAllowedToSelect
{
    
    private KArrayAggColumn() {
        super();
    }
    
    public KArrayAggColumn(
        final StringBuilder sb,
        final List<Object> params,
        final boolean closed
    ) {
        super(sb, params, closed);
    }
    
    private KArrayAggColumn(
        final StringBuilder sb,
        final List<Object> params,
        final boolean closed,
        final String name,
        final Class type,
        final KTable kTable
    ) {
        super(sb, params, closed, name, type, kTable);
    }
    
    public KColumn orderBy(
        final KColumnAllowedToOrderBy... kColumnsAllowedToOrderBy
    ) {
        KUtils.assertNotNullNotEmpty(kColumnsAllowedToOrderBy, "kColumnsAllowedToOrderBy", false);
        
        final List<KColumnAllowedToOrderBy> kColumnsAllowedToOrderByList =
            Arrays.asList(kColumnsAllowedToOrderBy)
                .stream()
                .filter(s-> s.getSqlToOrderBy() != null)
                .collect(Collectors.toList());
        
        if (kColumnsAllowedToOrderByList.isEmpty()) {
            return this;
        }
        
        final StringBuilder sbOrderBy = new StringBuilder(" ORDER BY ");
        
        for (int i = 0; i < kColumnsAllowedToOrderByList.size(); i++) {
            final KColumnAllowedToOrderBy kColumnAllowedToOrderBy = kColumnsAllowedToOrderByList.get(i);
            
            if (i > 0) {
                sbOrderBy.append(", ");
            }
            
            this.params.addAll(kColumnAllowedToOrderBy.getParams());
            sbOrderBy.append(kColumnAllowedToOrderBy.getSqlToOrderBy());
        }
        
        this.sb.insert(this.sb.length() - 1, sbOrderBy);
        
        return this;
    }
    
    @Override
    protected KArrayAggColumn cloneMe() {
        return new KArrayAggColumn(new StringBuilder(this.sb), new ArrayList<>(this.params), this.closed, this.name, this.type, this.kTable);
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
