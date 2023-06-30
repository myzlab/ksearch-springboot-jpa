package com.myzlab.k.crypto;

public class PgpOptions {
    
    private String cipherAlgo = null;
    private Integer compressAlgo = null;
    private Integer compressLevel = null;
    private Integer convertCrlf = null;
    private Integer disableMdc = null;
    private Integer sessKey = null;
    private Integer s2kMode = null;
    private Integer s2kCount = null;
    private String s2kDigestAlgo = null;
    private String s2kCipherAlgo = null;
    private Integer unicodeMode = null;
    
    public static PgpOptions create() {
        return new PgpOptions();
    }
    
    public String build() {
        final StringBuilder sb = new StringBuilder("");
        
        this.appendPropertyToStringBuilder(sb, this.cipherAlgo, "cipher-algo");
        this.appendPropertyToStringBuilder(sb, this.compressAlgo, "compress-algo");
        this.appendPropertyToStringBuilder(sb, this.compressLevel, "compress-level");
        this.appendPropertyToStringBuilder(sb, this.convertCrlf, "convert-crlf");
        this.appendPropertyToStringBuilder(sb, this.disableMdc, "disable-mdc");
        this.appendPropertyToStringBuilder(sb, this.sessKey, "sess-key");
        this.appendPropertyToStringBuilder(sb, this.s2kMode, "s2k-mode");
        this.appendPropertyToStringBuilder(sb, this.s2kCount, "s2k-count");
        this.appendPropertyToStringBuilder(sb, this.s2kDigestAlgo, "s2k-digest-algo");
        this.appendPropertyToStringBuilder(sb, this.s2kCipherAlgo, "s2k-cipher-algo");
        this.appendPropertyToStringBuilder(sb, this.unicodeMode, "unicode-mode");
        
        return sb.toString();
    }
    
    private void appendPropertyToStringBuilder(
        final StringBuilder sb,
        final Object value,
        final String name
    ) {
        if (value != null) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            
            sb.append(name).append("=").append(value);
        }
    }
    
    public static class CIPHER_ALGO {
        public static String BF = "bf";
        public static String AES128 = "aes128";
        public static String AES_192 = "aes192";
        public static String AES_256 = "aes256";
        public static String _3DES = "3des";
        public static String CAST5 = "cast5";
    }
    
    public static class COMPRESS_ALGO {
        public static int NO_COMPRESSION = 0;
        public static int ZIP_COMPRESSION = 1;
        public static int ZLIB_COMPRESSION = 2;
    }
    
    public static class S2K_MODE {
        public static int WITHOUT_SALT = 0;
        public static int WITH_SALT_BUT_WITH_FIXED_ITERATION_COUNT = 1;
        public static int VARIABLE_ITERATION_COUNT = 3;
    }
    
    public static class S2K_DIGEST_ALGO {
        public static String MD5 = "md5";
        public static String SHA1 = "sha1";
    }
    
    public static class S2K_CIPHER_ALGO {
        public static String BF = "bf";
        public static String AES = "aes";
        public static String AES128 = "aes128";
        public static String AES_192 = "aes192";
        public static String AES_256 = "aes256";
    }

    public PgpOptions setCipherAlgo(
        final String cipherAlgo
    ) {
        this.cipherAlgo = cipherAlgo;
        
        return this;
    }

    public PgpOptions setCompressAlgo(
        final  Integer compressAlgo
    ) {
        this.compressAlgo = compressAlgo;
        
        return this;
    }

    public PgpOptions setCompressLevel(
        final Integer compressLevel
    ) {
        this.compressLevel = compressLevel;
        
        return this;
    }

    public PgpOptions setConvertCrlf(
        final Integer convertCrlf
    ) {
        this.convertCrlf = convertCrlf;
        
        return this;
    }

    public PgpOptions setDisableMdc(
        final Integer disableMdc
    ) {
        this.disableMdc = disableMdc;
        
        return this;
    }

    public PgpOptions setSessKey(
        final Integer sessKey
    ) {
        this.sessKey = sessKey;
        
        return this;
    }

    public PgpOptions setS2kMode(
        final Integer s2kMode
    ) {
        this.s2kMode = s2kMode;
        
        return this;
    }

    public PgpOptions setS2kCount(
        final Integer s2kCount
    ) {
        this.s2kCount = s2kCount;
        
        return this;
    }

    public PgpOptions setS2kDigestAlgo(
        final String s2kDigestAlgo
    ) {
        this.s2kDigestAlgo = s2kDigestAlgo;
        
        return this;
    }

    public PgpOptions setS2kCipherAlgo(
        final String s2kCipherAlgo
    ) {
        this.s2kCipherAlgo = s2kCipherAlgo;
        
        return this;
    }

    public PgpOptions setUnicodeMode(
        final Integer unicodeMode
    ) {
        this.unicodeMode = unicodeMode;
        
        return this;
    }
    
    
}