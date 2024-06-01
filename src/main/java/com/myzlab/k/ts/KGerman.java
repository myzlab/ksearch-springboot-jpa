package com.myzlab.k.ts;

import com.myzlab.k.KTsConfig;

public class KGerman extends KTsConfig {
    
    @Override
    public String toSql() {
        return new StringBuilder().append("german").toString();
    }
}
