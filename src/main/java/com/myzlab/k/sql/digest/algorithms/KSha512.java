package com.myzlab.k.sql.digest.algorithms;

import com.myzlab.k.KDigestAlgorithm;

public class KSha512 extends KDigestAlgorithm {
    
    @Override
    public String toSql() {
        return "SHA512";
    }
}
