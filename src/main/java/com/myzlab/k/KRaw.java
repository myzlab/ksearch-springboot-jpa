package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToGroupBy;
import com.myzlab.k.allowed.KColumnAllowedToOrderBy;
import com.myzlab.k.allowed.KColumnAllowedToReturning;
import com.myzlab.k.allowed.KColumnAllowedToSelect;
import com.myzlab.k.allowed.KColumnAllowedToSetUpdate;
import java.util.ArrayList;
import java.util.List;

public class KRaw implements
    KColumnAllowedToOrderBy,
    KColumnAllowedToGroupBy,
    KColumnAllowedToReturning,
    KColumnAllowedToSetUpdate,
    KColumnAllowedToSelect
{

    protected String content;
    protected final List<Object> params = new ArrayList<>();

    public KRaw(
        final String content,
        final Object... args
    ) {
        if (args != null && args.length > 0) {
            this.content = String.format(content, args);
        } else {
            this.content = content;
        }
    }
    
    public KRaw addParam(
        final Object param
    ) {
        this.params.add(param);
        
        return this;
    }

    @Override
    public String getSqlToOrderBy() {
        return content;
    }

    @Override
    public String getSqlToGroupBy() {
        return content;
    }
    
    @Override
    public String getSqlToReturning() {
        return content;
    }

    @Override
    public List<Object> getParams() {
        return this.params;
    }

    @Override
    public String getSqlToSet() {
        return content;
    }
    
    @Override
    public KBaseColumn getKBaseColumn() {
        return new KColumn(new StringBuilder(content), this.params, false);
    }
}
