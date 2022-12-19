package com.myzlab.k;

import java.util.ArrayList;
import java.util.List;

public class KCase {
    
    private final StringBuilder sb = new StringBuilder();
    private final List<Object> params = new ArrayList<>();

    private KCase() {
        super();
        
        this.process();
    }
    
    public static KCase getInstance() {
        return new KCase();
    }
    
    public static KCase getInstance(
        final StringBuilder sb,
        final List<Object> params
    ) {
        return new KCase();
    }
    
    public KCaseWhen when(
        final KCondition kCondition
    ) {
        return KCaseWhen.getInstance(this.sb, this.params, kCondition);
    }
    
    private void process() {
        this.sb.append("CASE");
    }
}
