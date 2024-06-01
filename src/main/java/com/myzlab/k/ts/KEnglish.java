package com.myzlab.k.ts;

import com.myzlab.k.KTsConfig;

public class KEnglish extends KTsConfig {
    
    @Override
    public String toSql() {
        return new StringBuilder().append("english").toString();
    }
}
