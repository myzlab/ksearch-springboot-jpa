package com.myzlab.k.sql.digest.algorithms;

import com.myzlab.k.KDigestAlgorithm;

public class KSha1 extends KDigestAlgorithm {
    
    @Override
    public String toSql() {
        return "SHA1";
    }
}
