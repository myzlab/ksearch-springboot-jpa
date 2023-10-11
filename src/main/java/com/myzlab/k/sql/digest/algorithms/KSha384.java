package com.myzlab.k.sql.digest.algorithms;

import com.myzlab.k.KDigestAlgorithm;

public class KSha384 extends KDigestAlgorithm {
    
    @Override
    public String toSql() {
        return "SHA384";
    }
}
