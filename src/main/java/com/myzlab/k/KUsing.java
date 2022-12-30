package com.myzlab.k;

import com.myzlab.k.helper.KExceptionHelper;

public class KUsing extends KQueryDelete {

    private KUsing() {
        super();
    }
    
    private KUsing(
        final KExecutor kExecutor,
        final KQueryDeleteData kQueryDeleteData,
        final KTable kTable
    ) {
        super(kQueryDeleteData, kExecutor);
        
        this.process(kTable);
    }
    
    protected static KUsing getInstance(
        final KExecutor kExecutor,
        final KQueryDeleteData kQueryDeleteData,
        final KTable kTable
    ) {
        return new KUsing(kExecutor, kQueryDeleteData, kTable);
    }
    
    public KUsing using(
        final KTable kTable
    ) {
        this.process(kTable);
        
        return this;
    }
    
    public KUsing using(
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        this.process(new KTable(null, kRaw.content, null));
        
        return this;
    }
    
    public KUsing using(
        final KCommonTableExpressionFilled kCommonTableExpressionFilled
    ) {
        this.process(new KTable(null, kCommonTableExpressionFilled.name, kCommonTableExpressionFilled.alias));
        
        return this;
    }
    
    public KWhereDelete where(
        final KCondition kCondition
    ) {
        return KWhereDelete.getInstance(this.k, this.kQueryDeleteData, kCondition);
    }
    
    public KWhereDelete where(
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        final KCondition kCondition = new KCondition(kRaw.content);
        
        return KWhereDelete.getInstance(this.k, this.kQueryDeleteData, kCondition);
    }
    
    private void process(
        final KTable kTable
    ) {
        if (kTable == null) {
            throw KExceptionHelper.internalServerError("The 'kTable' param is required"); 
        }
        
        if (this.kQueryDeleteData.usingTablesAdded == 0) {
            this.kQueryDeleteData.sb.append(" USING ");
        } else {
            this.kQueryDeleteData.sb.append(", ");
        }
        
        this.kQueryDeleteData.usingTablesAdded++;
        
        if (kTable.kQueryData != null) {
            this.kQueryDeleteData.params.addAll(kTable.kQueryData.params);
        }
            
        this.kQueryDeleteData.sb.append(kTable.toSql(true));
    }
}