package com.myzlab.k.sql.digest.algorithms;

import com.myzlab.k.KDigestAlgorithm;

public class KSha224 extends KDigestAlgorithm {
    
    @Override
    public String toSql() {
        return "SHA224";
    }
}
