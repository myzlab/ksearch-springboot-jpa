package com.myzlab.k.allowed;

import java.util.List;

public interface KColumnAllowedToReturning {
    
    public abstract String getSqlToReturning();
    public abstract List<Object> getParams();
}
