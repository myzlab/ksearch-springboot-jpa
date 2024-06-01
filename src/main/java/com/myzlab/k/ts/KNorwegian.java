package com.myzlab.k.ts;

import com.myzlab.k.KTsConfig;

public class KNorwegian extends KTsConfig {
    
    @Override
    public String toSql() {
        return new StringBuilder().append("norwegian").toString();
    }
}
