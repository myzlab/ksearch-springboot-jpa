package com.myzlab.k;

import com.myzlab.k.helper.KExceptionHelper;
import java.util.ArrayList;
import java.util.List;

public class KCaseWhen {
    
    private final StringBuilder sb = new StringBuilder();
    private final List<Object> params = new ArrayList<>();

    private KCaseWhen() {
        super();
    }
    
    protected KCaseWhen(
        final StringBuilder sb,
        final List<Object> params,
        final KCondition kCondition
    ) {
        super();
        
        this.sb.append(sb);
        this.params.addAll(params);
        
        this.process(kCondition);
    }
    
    public static KCaseWhen getInstance() {
        return new KCaseWhen();
    }
    
    public static KCaseWhen getInstance(
        final StringBuilder sb,
        final List<Object> params,
        final KCondition kCondition
    ) {
        return new KCaseWhen(sb, params, kCondition);
    }
    
    public KCasePartial then(
        final KBaseColumnCastable kBaseColumnCastable
    ) {
        return KCasePartial.getInstance(this.sb, this.params, kBaseColumnCastable);
    }
    
    public KCasePartial then(
        final KRaw kRaw
    ) {
        return KCasePartial.getInstance(this.sb, this.params, kRaw);
    }
    
    private void process(
        final KCondition kCondition
    ) {
        KUtils.assertNotNull(kCondition, "kCondition");
        
        if (kCondition.emptyCondition) {
            throw KExceptionHelper.internalServerError("An empty condition is not allowed in case expression");
        }
        
        this.sb.append(" WHEN ").append(kCondition.toSql());
        this.params.addAll(kCondition.params);
    }
}
