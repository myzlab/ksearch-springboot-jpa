package com.myzlab.k.allowed;

import java.util.List;

public interface KColumnAllowedToSetUpdate {
    
    public abstract String getSqlToSet();
    public abstract List<Object> getParams();
}
