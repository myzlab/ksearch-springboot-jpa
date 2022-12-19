package com.myzlab.k;

import com.myzlab.k.sql.extract.field.KCentury;
import com.myzlab.k.sql.extract.field.KDay;
import com.myzlab.k.sql.extract.field.KDecade;
import com.myzlab.k.sql.extract.field.KDow;
import com.myzlab.k.sql.extract.field.KDoy;
import com.myzlab.k.sql.extract.field.KEpoch;
import com.myzlab.k.sql.extract.field.KHour;
import com.myzlab.k.sql.extract.field.KMicroseconds;
import com.myzlab.k.sql.extract.field.KMillennium;
import com.myzlab.k.sql.extract.field.KMilliseconds;
import com.myzlab.k.sql.extract.field.KMinute;
import com.myzlab.k.sql.extract.field.KMonth;
import com.myzlab.k.sql.extract.field.KQuarter;
import com.myzlab.k.sql.extract.field.KSecond;
import com.myzlab.k.sql.extract.field.KWeek;
import com.myzlab.k.sql.extract.field.KYear;

public class SqlExtractField {
    
    public static KCentury century() {
        return new KCentury();
    }
    
    public static KDay day() {
        return new KDay();
    }
    
    public static KDecade decade() {
        return new KDecade();
    }
    
    public static KDow dow() {
        return new KDow();
    }
    
    public static KDoy doy() {
        return new KDoy();
    }
    
    public static KEpoch epoch() {
        return new KEpoch();
    }
    
    public static KHour hour() {
        return new KHour();
    }
    
    public static KMicroseconds microseconds() {
        return new KMicroseconds();
    }
    
    public static KMillennium millennium() {
        return new KMillennium();
    }
    
    public static KMilliseconds milliseconds() {
        return new KMilliseconds();
    }
    
    public static KMinute minute() {
        return new KMinute();
    }
    
    public static KMonth month() {
        return new KMonth();
    }
    
    public static KQuarter quarter() {
        return new KQuarter();
    }
    
    public static KSecond second() {
        return new KSecond();
    }
    
    public static KWeek week() {
        return new KWeek();
    }
    
    public static KYear year() {
        return new KYear();
    }
}
