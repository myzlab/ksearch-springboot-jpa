package com.myzlab.k.sql.digest.algorithms;

import com.myzlab.k.KDigestAlgorithm;

public class KMd5 extends KDigestAlgorithm {
    
    @Override
    public String toSql() {
        return "MD5";
    }
}
