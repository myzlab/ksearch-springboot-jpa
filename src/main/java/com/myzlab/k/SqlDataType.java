package com.myzlab.k;

import com.myzlab.k.sql.datatype.KBigint;
import com.myzlab.k.sql.datatype.KBigserial;
import com.myzlab.k.sql.datatype.KBit;
import com.myzlab.k.sql.datatype.KBitVarying;
import com.myzlab.k.sql.datatype.KBoolean;
import com.myzlab.k.sql.datatype.KBytea;
import com.myzlab.k.sql.datatype.KCharacter;
import com.myzlab.k.sql.datatype.KCharacterVarying;
import com.myzlab.k.sql.datatype.KDate;
import com.myzlab.k.sql.datatype.KDoublePrecision;
import com.myzlab.k.sql.datatype.KInteger;
import com.myzlab.k.sql.datatype.KJson;
import com.myzlab.k.sql.datatype.KJsonb;
import com.myzlab.k.sql.datatype.KNumeric;
import com.myzlab.k.sql.datatype.KReal;
import com.myzlab.k.sql.datatype.KSerial;
import com.myzlab.k.sql.datatype.KSmallint;
import com.myzlab.k.sql.datatype.KSmallserial;
import com.myzlab.k.sql.datatype.KText;
import com.myzlab.k.sql.datatype.KTimeWithTimeZone;
import com.myzlab.k.sql.datatype.KTimeWithoutTimeZone;
import com.myzlab.k.sql.datatype.KTimestampWithTimeZone;
import com.myzlab.k.sql.datatype.KTimestampWithoutTimeZone;
import com.myzlab.k.sql.datatype.KUuid;

public class SqlDataType {
    
    public static KBytea bytea() {
        return new KBytea();
    }

    public static KBigint bigint() {
        return new KBigint();
    }
    
    public static KBigserial bigserial() {
        return new KBigserial();
    }
    
    public static KBit bit() {
        return new KBit();
    }
    
    public static KBit bit(
        final int length
    ) {
        return new KBit(length);
    }
    
    public static KBitVarying bitVarying() {
        return new KBitVarying();
    }
    
    public static KBitVarying bitVarying(
        final int length
    ) {
        return new KBitVarying(length);
    }
    
    public static KBoolean bool() {
        return new KBoolean();
    }
    
    public static KCharacter character() {
        return new KCharacter();
    }
    
    public static KCharacter character(
        final int length
    ) {
        return new KCharacter(length);
    }
    
    public static KCharacterVarying characterVarying() {
        return new KCharacterVarying();
    }
    
    public static KCharacterVarying characterVarying(
        final int length
    ) {
        return new KCharacterVarying(length);
    }
    
    public static KDate date() {
        return new KDate();
    }
    
    public static KNumeric decimal() {
        return numeric();
    }
    
    public static KNumeric decimal(
        final int precision
    ) {
        return numeric(precision);
    }
    
    public static KNumeric decimal(
        final int precision,
        final int scale
    ) {
        return numeric(precision, scale);
    }
    
    public static KDoublePrecision doublePrecision() {
        return new KDoublePrecision();
    }
    
    public static KReal float4() {
        return real();
    }
    
    public static KDoublePrecision float8() {
        return doublePrecision();
    }
    
    public static KJson json() {
        return new KJson();
    }
    
    public static KJsonb jsonb() {
        return new KJsonb();
    }
    
    public static KSmallint int2() {
        return smallint();
    }
    
    public static KInteger int4() {
        return integer();
    }
    
    public static KBigint int8() {
        return bigint();
    }
    
    public static KInteger integer() {
        return new KInteger();
    }
    
    public static KNumeric numeric() {
        return new KNumeric();
    }
    
    public static KNumeric numeric(
        final int precision
    ) {
        return new KNumeric(precision);
    }
    
    public static KNumeric numeric(
        final int precision,
        final int scale
    ) {
        return new KNumeric(precision, scale);
    }
    
    public static KReal real() {
        return new KReal();
    }
    
    public static KSmallserial smallserial() {
        return new KSmallserial();
    }
    
    public static KSerial serial() {
        return new KSerial();
    }
    
    public static KSmallserial serial2() {
        return smallserial();
    }
    
    public static KSerial serial4() {
        return serial();
    }
    
    public static KBigserial serial8() {
        return bigserial();
    }
    
    public static KSmallint smallint() {
        return new KSmallint();
    }
    
    public static KText text() {
        return new KText();
    }
    
    public static KTimeWithoutTimeZone time() {
        return timeWithoutTimeZone();
    }
    
    public static KTimestampWithoutTimeZone timestamp() {
        return timestampWithoutTimeZone();
    }
    
    public static KTimestampWithTimeZone timestamptz() {
        return timestampWithTimeZone();
    }
    
    public static KTimestampWithoutTimeZone timestampWithoutTimeZone() {
        return new KTimestampWithoutTimeZone();
    }
    
    public static KTimestampWithTimeZone timestampWithTimeZone() {
        return new KTimestampWithTimeZone();
    }
    
    public static KTimeWithTimeZone timetz() {
        return timeWithTimeZone();
    }
    
    public static KTimeWithoutTimeZone timeWithoutTimeZone() {
        return new KTimeWithoutTimeZone();
    }
    
    public static KTimeWithTimeZone timeWithTimeZone() {
        return new KTimeWithTimeZone();
    }
    
    public static KUuid uuid() {
        return new KUuid();
    }
    
    public static KBitVarying varbit() {
        return bitVarying();
    }
    
    public static KBitVarying varbit(
        final int length
    ) {
        return bitVarying(length);
    }
    
    public static KCharacterVarying varchar() {
        return characterVarying();
    }
    
    public static KCharacterVarying varchar(
        final int length
    ) {
        return characterVarying(length);
    }
}
