package com.myzlab.k;

public class KIntervalFilled {
    
    private final StringBuilder sb = new StringBuilder();

    protected KIntervalFilled(
        final StringBuilder sb,
        final Double value,
        final String unit
    ) {
        super();
        
        this.sb.append(sb);
        
        this.process(value, unit);
    }
    
    public static KIntervalFilled getInstance(
        final StringBuilder sb,
        final Double value,
        final String unit
    ) {
        return new KIntervalFilled(sb, value, unit);
    }
    
    public KIntervalFilled years(
        final Double value
    ) {
        return KIntervalFilled.getInstance(this.sb, value, "years");
    }
    
    public KIntervalFilled quarters(
        final Double value
    ) {
        return KIntervalFilled.getInstance(this.sb, value, "quarters");
    }
    
    public KIntervalFilled months(
        final Double value
    ) {
        return KIntervalFilled.getInstance(this.sb, value, "months");
    }
    
    public KIntervalFilled weeks(
        final Double value
    ) {
        return KIntervalFilled.getInstance(this.sb, value, "weeks");
    }
    
    public KIntervalFilled days(
        final Double value
    ) {
        return KIntervalFilled.getInstance(this.sb, value, "days");
    }
    
    public KIntervalFilled hours(
        final Double value
    ) {
        return KIntervalFilled.getInstance(this.sb, value, "hours");
    }
    
    public KIntervalFilled minutes(
        final Double value
    ) {
        return KIntervalFilled.getInstance(this.sb, value, "minutes");
    }
    
    public KIntervalFilled seconds(
        final Double value
    ) {
        return KIntervalFilled.getInstance(this.sb, value, "seconds");
    }
    
    public KIntervalFilled milliseconds(
        final Double value
    ) {
        return KIntervalFilled.getInstance(this.sb, value, "milliseconds");
    }
    
    public KIntervalFilled microseconds(
        final Double value
    ) {
        return KIntervalFilled.getInstance(this.sb, value, "microseconds");
    }
    
    public KIntervalClosed close() {
        return KIntervalClosed.getInstance(this.sb);
    }
    
    private void process(
        final Double value,
        final String unit
    ) {
        KUtils.assertNotNull(value, "value");
        
        if (this.sb.length() > 12) {
            this.sb.append(" ");
        }
        
        this.sb.append(value.toString()).append(" ").append(unit);
    }
}