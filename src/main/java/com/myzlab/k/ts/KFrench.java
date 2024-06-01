package com.myzlab.k.ts;

import com.myzlab.k.KTsConfig;

public class KFrench extends KTsConfig {
    
    @Override
    public String toSql() {
        return new StringBuilder().append("french").toString();
    }
}
