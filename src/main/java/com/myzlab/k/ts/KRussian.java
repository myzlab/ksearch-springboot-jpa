package com.myzlab.k.ts;

import com.myzlab.k.KTsConfig;

public class KRussian extends KTsConfig {
    
    @Override
    public String toSql() {
        return new StringBuilder().append("russian").toString();
    }
}
