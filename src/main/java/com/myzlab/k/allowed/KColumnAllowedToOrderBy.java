package com.myzlab.k.allowed;

import java.util.List;

public interface KColumnAllowedToOrderBy {
    
    public abstract String getSqlToOrderBy();
    public abstract List<Object> getParams();
}
