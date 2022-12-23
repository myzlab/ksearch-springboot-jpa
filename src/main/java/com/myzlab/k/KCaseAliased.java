package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToSelect;
import java.util.ArrayList;
import java.util.List;

public class KCaseAliased implements KColumnAllowedToSelect {
    
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
}
