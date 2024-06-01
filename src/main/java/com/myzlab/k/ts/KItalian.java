package com.myzlab.k.ts;

import com.myzlab.k.KTsConfig;

public class KItalian extends KTsConfig {
    
    @Override
    public String toSql() {
        return new StringBuilder().append("italian").toString();
    }
}
