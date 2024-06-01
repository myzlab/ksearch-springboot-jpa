package com.myzlab.k.ts;

import com.myzlab.k.KTsConfig;

public class KDanish extends KTsConfig {
    
    @Override
    public String toSql() {
        return new StringBuilder().append("danish").toString();
    }
}
