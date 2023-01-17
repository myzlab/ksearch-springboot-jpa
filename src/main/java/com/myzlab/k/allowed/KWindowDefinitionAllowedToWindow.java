package com.myzlab.k.allowed;

import java.util.List;

public interface KWindowDefinitionAllowedToWindow {
    
    public abstract String getName();
    public abstract String getSql();
    public abstract List<Object> getParams();
    
}
