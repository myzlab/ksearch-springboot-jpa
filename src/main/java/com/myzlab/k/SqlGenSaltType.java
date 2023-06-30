package com.myzlab.k;

import com.myzlab.k.sql.gen.salt.type.KBf;
import com.myzlab.k.sql.gen.salt.type.KDes;
import com.myzlab.k.sql.gen.salt.type.KMd5;
import com.myzlab.k.sql.gen.salt.type.KXdes;

public class SqlGenSaltType {
    
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
