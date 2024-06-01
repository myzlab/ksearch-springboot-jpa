package com.myzlab.k.ts;

import com.myzlab.k.KTsConfig;

public class KSwedish extends KTsConfig {
    
    @Override
    public String toSql() {
        return new StringBuilder().append("swedish").toString();
    }
}
