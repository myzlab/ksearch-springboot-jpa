package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToReturning;
import com.myzlab.k.helper.KExceptionHelper;
import com.myzlab.k.optional.KOptionalKCondition;

public class KWhereDelete extends KQueryDelete {
    
    private final KCondition kCondition;
    
    private KWhereDelete() {
        super();
        
        this.kCondition = null;
    }
    
    private KWhereDelete(
        final KExecutor kExecutor,
        final KQueryDeleteData kQueryDeleteData,
        final KCondition kCondition
    ) {
        super(kQueryDeleteData, kExecutor);
        
        KUtils.assertNotNull(kCondition, "kCondition");
        
        this.kCondition = kCondition;
    }
    
    protected static KWhereDelete getInstance(
        final KExecutor kExecutor,
        final KQueryDeleteData kQueryDeleteData,
        final KCondition kCondition
    ) {
        return new KWhereDelete(kExecutor, kQueryDeleteData, kCondition);
    }
    
    public KWhereDelete andNot(
        final KCondition kCondition
    ) {
        this.kCondition.andNot(kCondition);
        
        return this;
    }
    
    public KWhereDelete andNot(
        final KOptionalKCondition kOptionalKCondition
    ) {
        this.kCondition.andNot(!kOptionalKCondition.isPresent() ? KCondition.getEmptyInstance() : kOptionalKCondition.get());
        
        return this;
    }
    
    public KWhereDelete andNot(
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        final KCondition kRawCondition = new KCondition(kRaw.content, kRaw.params);
        
        this.kCondition.andNot(kRawCondition);
        
        return this;
    }
    
    public KWhereDelete orNot(
        final KCondition kCondition
    ) {
        this.kCondition.orNot(kCondition);
        
        return this;
    }
    
    public KWhereDelete orNot(
        final KOptionalKCondition kOptionalKCondition
    ) {
        this.kCondition.orNot(!kOptionalKCondition.isPresent() ? KCondition.getEmptyInstance() : kOptionalKCondition.get());
        
        return this;
    }
    
    public KWhereDelete orNot(
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        final KCondition kRawCondition = new KCondition(kRaw.content, kRaw.params);
        
        this.kCondition.orNot(kRawCondition);
        
        return this;
    }

    public KWhereDelete and(
        final KCondition kCondition
    ) {
        this.kCondition.and(kCondition);
        
        return this;
    }
    
    public KWhereDelete and(
        final KOptionalKCondition kOptionalKCondition
    ) {
        this.kCondition.and(!kOptionalKCondition.isPresent() ? KCondition.getEmptyInstance() : kOptionalKCondition.get());
        
        return this;
    }
    
    public KWhereDelete and(
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        final KCondition kRawCondition = new KCondition(kRaw.content, kRaw.params);
        
        this.kCondition.and(kRawCondition);
        
        return this;
    }
    
    public KWhereDelete or(
        final KCondition kCondition
    ) {
        this.kCondition.or(kCondition);
        
        return this;
    }
    
    public KWhereDelete or(
        final KOptionalKCondition kOptionalKCondition
    ) {
        this.kCondition.or(!kOptionalKCondition.isPresent() ? KCondition.getEmptyInstance() : kOptionalKCondition.get());
        
        return this;
    }
    
    public KWhereDelete or(
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        final KCondition kRawCondition = new KCondition(kRaw.content, kRaw.params);
        
        this.kCondition.or(kRawCondition);
        
        return this;
    }
    
    public KReturningDelete returning(
        final KColumnAllowedToReturning... kColumnsAllowedToReturning
    ) {
        this.buildWhere();
        
        return KReturningDelete.getInstance(this.k, this.kQueryDeleteData, kColumnsAllowedToReturning);
    }
    
    private void buildWhere() {
        this.buildWhere(this.kQueryDeleteData);
    }

    private void buildWhere(
        final KQueryDeleteData kQueryDeleteData
    ) {
        KUtils.assertNotNull(this.kCondition, "kCondition");
        
        if (this.kCondition.emptyCondition) {
            throw KExceptionHelper.internalServerError("'DELETE' sentence without conditions is not allowed");
        }
        
        kQueryDeleteData.sb.append(" WHERE ").append(this.kCondition.toSql());
        kQueryDeleteData.params.addAll(this.kCondition.params);
    }
    
    public int execute() {
        this.buildWhere();
        
        return super.executeSingle();
    }
}