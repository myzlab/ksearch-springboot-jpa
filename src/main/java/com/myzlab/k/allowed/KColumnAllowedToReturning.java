package com.myzlab.k.allowed;

import com.myzlab.k.KBaseColumn;
import java.util.List;

public interface KColumnAllowedToReturning {
    
    public abstract String getSqlToReturning();
    public abstract List<Object> getParams();
    public abstract KBaseColumn getKBaseColumn();
    
}
