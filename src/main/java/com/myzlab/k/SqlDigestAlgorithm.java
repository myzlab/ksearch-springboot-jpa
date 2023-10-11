package com.myzlab.k;

import com.myzlab.k.sql.digest.algorithms.KMd5;
import com.myzlab.k.sql.digest.algorithms.KSha1;
import com.myzlab.k.sql.digest.algorithms.KSha224;
import com.myzlab.k.sql.digest.algorithms.KSha256;
import com.myzlab.k.sql.digest.algorithms.KSha384;
import com.myzlab.k.sql.digest.algorithms.KSha512;

public class SqlDigestAlgorithm {
    
    public static KMd5 md5() {
        return new KMd5();
    }

    public static KSha1 sha1() {
        return new KSha1();
    }
    
    public static KSha224 sha224() {
        return new KSha224();
    }
    
    public static KSha256 sha256() {
        return new KSha256();
    }
    
    public static KSha384 sha384() {
        return new KSha384();
    }
    
    public static KSha512 sha512() {
        return new KSha512();
    }
    
}
