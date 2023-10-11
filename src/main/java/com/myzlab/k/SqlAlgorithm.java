package com.myzlab.k;

import com.myzlab.k.sql.algorithms.KBf;
import com.myzlab.k.sql.algorithms.KDes;
import com.myzlab.k.sql.algorithms.KMd5;
import com.myzlab.k.sql.algorithms.KXdes;

public class SqlAlgorithm {
    
    public static KDes des() {
        return new KDes();
    }

    public static KXdes xdes() {
        return new KXdes();
    }
    
    public static KMd5 md5() {
        return new KMd5();
    }
    
    public static KBf bf() {
        return new KBf();
    }
    
}
