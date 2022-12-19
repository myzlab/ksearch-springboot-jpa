package com.myzlab.k;

import com.myzlab.k.sql.format.KBase64;
import com.myzlab.k.sql.format.KEscape;
import com.myzlab.k.sql.format.KHex;

public class SqlFormat {
    
    public static KBase64 base64() {
        return new KBase64();
    }
    
    public static KEscape escape() {
        return new KEscape();
    }
    
    public static KHex hex() {
        return new KHex();
    }
}
