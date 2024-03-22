package com.myzlab.k;

public class KInterval {
    
    private final StringBuilder sb = new StringBuilder();

    private KInterval() {
        super();
        
        this.process();
    }
    
    public static KInterval getInstance() {
        return new KInterval();
    }
    
    public KIntervalFilled years(
        final Double value
    ) {
        return KIntervalFilled.getInstance(this.sb, value, "years");
    }
    
    public KIntervalFilled years(
        final Integer value
    ) {
        return KIntervalFilled.getInstance(this.sb, value, "years");
    }
    
    public KIntervalFilled quarters(
        final Double value
    ) {
        return KIntervalFilled.getInstance(this.sb, value, "quarters");
    }
    
    public KIntervalFilled quarters(
        final Integer value
    ) {
        return KIntervalFilled.getInstance(this.sb, value, "quarters");
    }
    
    public KIntervalFilled months(
        final Double value
    ) {
        return KIntervalFilled.getInstance(this.sb, value, "months");
    }
    
    public KIntervalFilled months(
        final Integer value
    ) {
        return KIntervalFilled.getInstance(this.sb, value, "months");
    }
    
    public KIntervalFilled weeks(
        final Double value
    ) {
        return KIntervalFilled.getInstance(this.sb, value, "weeks");
    }
    
    public KIntervalFilled weeks(
        final Integer value
    ) {
        return KIntervalFilled.getInstance(this.sb, value, "weeks");
    }
    
    public KIntervalFilled days(
        final Double value
    ) {
        return KIntervalFilled.getInstance(this.sb, value, "days");
    }
    
    public KIntervalFilled days(
        final Integer value
    ) {
        return KIntervalFilled.getInstance(this.sb, value, "days");
    }
    
    public KIntervalFilled hours(
        final Double value
    ) {
        return KIntervalFilled.getInstance(this.sb, value, "hours");
    }
    
    public KIntervalFilled hours(
        final Integer value
    ) {
        return KIntervalFilled.getInstance(this.sb, value, "hours");
    }
    
    public KIntervalFilled minutes(
        final Double value
    ) {
        return KIntervalFilled.getInstance(this.sb, value, "minutes");
    }
    
    public KIntervalFilled minutes(
        final Integer value
    ) {
        return KIntervalFilled.getInstance(this.sb, value, "minutes");
    }
    
    public KIntervalFilled seconds(
        final Double value
    ) {
        return KIntervalFilled.getInstance(this.sb, value, "seconds");
    }
    
    public KIntervalFilled seconds(
        final Integer value
    ) {
        return KIntervalFilled.getInstance(this.sb, value, "seconds");
    }
    
    public KIntervalFilled milliseconds(
        final Double value
    ) {
        return KIntervalFilled.getInstance(this.sb, value, "milliseconds");
    }
    
    public KIntervalFilled milliseconds(
        final Integer value
    ) {
        return KIntervalFilled.getInstance(this.sb, value, "milliseconds");
    }
    
    public KIntervalFilled microseconds(
        final Double value
    ) {
        return KIntervalFilled.getInstance(this.sb, value, "microseconds");
    }
    
    public KIntervalFilled microseconds(
        final Integer value
    ) {
        return KIntervalFilled.getInstance(this.sb, value, "microseconds");
    }
    
    private void process() {
        this.sb.append("INTERVAL '");
    }
}