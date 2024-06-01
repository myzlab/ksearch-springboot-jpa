package com.myzlab.k.ts;

import com.myzlab.k.KTsConfig;

public class KDutch extends KTsConfig {
    
    @Override
    public String toSql() {
        return new StringBuilder().append("dutch").toString();
    }
}
