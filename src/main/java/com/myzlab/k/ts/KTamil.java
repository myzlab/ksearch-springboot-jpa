package com.myzlab.k.ts;

import com.myzlab.k.KTsConfig;

public class KTamil extends KTsConfig {
    
    @Override
    public String toSql() {
        return new StringBuilder().append("tamil").toString();
    }
}
