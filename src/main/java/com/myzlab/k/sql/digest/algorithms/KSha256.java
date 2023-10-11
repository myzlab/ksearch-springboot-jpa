package com.myzlab.k.sql.digest.algorithms;

import com.myzlab.k.KDigestAlgorithm;

public class KSha256 extends KDigestAlgorithm {
    
    @Override
    public String toSql() {
        return "SHA256";
    }
}
