package com.myzlab.k;

import com.myzlab.k.helper.KExceptionHelper;

public class KFromUpdate extends KQueryUpdate {

    private KFromUpdate() {
        super();
    }
    
    private KFromUpdate(
        final KExecutor kExecutor,
        final KQueryUpdateData kQueryUpdateData,
        final KTable kTable
    ) {
        super(kQueryUpdateData, kExecutor);
        
        this.process(kTable);
    }
    
    protected static KFromUpdate getInstance(
        final KExecutor kExecutor,
        final KQueryUpdateData kQueryUpdateData,
        final KTable kTable
    ) {
        return new KFromUpdate(kExecutor, kQueryUpdateData, kTable);
    }
    
    public KFromUpdate from(
        final KTable kTable
    ) {
        this.process(kTable);
        
        return this;
    }
    
    public KFromUpdate from(
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        this.process(new KTable(null, kRaw.content, null));
        
        return this;
    }
    
    public KFromUpdate from(
        final KCommonTableExpressionFilled kCommonTableExpressionFilled
    ) {
        this.process(new KTable(null, kCommonTableExpressionFilled.name, kCommonTableExpressionFilled.alias));
        
        return this;
    }
    
    public KWhereUpdate where(
        final KCondition kCondition
    ) {
        return KWhereUpdate.getInstance(this.k, this.kQueryUpdateData, kCondition);
    }
    
    public KWhereUpdate where(
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        final KCondition kCondition = new KCondition(kRaw.content);
        
        return KWhereUpdate.getInstance(this.k, this.kQueryUpdateData, kCondition);
    }
    
    private void process(
        final KTable kTable
    ) {
        if (kTable == null) {
            throw KExceptionHelper.internalServerError("The 'kTable' param is required"); 
        }
        
        if (this.kQueryUpdateData.fromTablesAdded == 0) {
            this.kQueryUpdateData.sb.append(" FROM ");
        } else {
            this.kQueryUpdateData.sb.append(", ");
        }
        
        this.kQueryUpdateData.fromTablesAdded++;
        
        if (kTable.kQueryData != null) {
            this.kQueryUpdateData.params.addAll(kTable.kQueryData.params);
        }
            
        this.kQueryUpdateData.sb.append(kTable.toSql(true));
    }
}