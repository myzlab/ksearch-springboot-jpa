package com.myzlab.k;

import java.util.HashMap;
import java.util.Map;

public class DynamicMap {
    
    private final HashMap<String, Object> data;

    public DynamicMap() {
        this.data = new HashMap<>();
    }
    
    public static DynamicMap create() {
        return new DynamicMap();
    }
    
    public DynamicMap add(final String name, final Object value) {
        this.data.put(name, value);
        
        return this;
    }
    
    public DynamicMap add(final Map<String, ?> map) {
        if (map == null) {
            return this;
        }
        
        for (final Map.Entry<String, ?> entry : map.entrySet()) {            
            if (entry.getValue() != null) {
                this.add(entry.getKey(), entry.getValue());
            }
        }
        
        return this;
    }

    public HashMap<String, Object> build() {
        return data;
    }
    
    public HashMap<String, String> buildWithStringValue() {
        final HashMap<String, String> map = new HashMap(data);
        
        return map;
    }
}
