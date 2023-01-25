package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToReturning;
import com.myzlab.k.allowed.KColumnAllowedToSelect;
import com.myzlab.k.allowed.KColumnAllowedToSetUpdate;
import java.util.ArrayList;
import java.util.List;

public class KCaseAliased implements
    KColumnAllowedToSelect,
    KColumnAllowedToReturning,
    KColumnAllowedToSetUpdate
{
    
    private final StringBuilder sb = new StringBuilder();
    private final List<Object> params = new ArrayList<>();
    private final String alias;

    protected KCaseAliased(
        final StringBuilder sb,
        final List<Object> params,
        final String alias
    ) {
        super();
        
        this.sb.append(sb);
        this.params.addAll(params);
        this.alias = alias;
        
        this.process();
    }
    
    public static KCaseAliased getInstance(
        final StringBuilder sb,
        final List<Object> params,
        final String alias
    ) {
        return new KCaseAliased(sb, params, alias);
    }
    
    private void process() {
        this.sb.append(" END");
    }

    @Override
    public KBaseColumn getKBaseColumn() {
        return new KAliasedColumn(new StringBuilder(this.sb), this.alias, this.params, false);
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
}
