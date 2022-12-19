package com.myzlab.k.allowed;

import java.util.List;

public interface KColumnAllowedToGroupBy {
    
    public abstract String getSqlToGroupBy();
    public abstract List<Object> getParams();
}
