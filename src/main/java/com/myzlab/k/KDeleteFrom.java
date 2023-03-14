package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToReturning;
import com.myzlab.k.helper.KExceptionHelper;

public class KDeleteFrom extends KQueryDelete {

    private KDeleteFrom(
        final KExecutor kExecutor
    ) {
        super(kExecutor);
    }
 
    private KDeleteFrom(
        final KExecutor kExecutor,
        final KTable kTable
    ) {
        super(kExecutor);
        
        this.process(kTable);
    }
    
    private KDeleteFrom(
        final KExecutor kExecutor,
        final KQueryDeleteData kQueryDeleteData,
        final KTable kTable
    ) {
        super(kQueryDeleteData, kExecutor);
        
        this.process(kTable);
    }
    
    protected static KDeleteFrom getInstance(
        final KExecutor kExecutor,
        final KTable kTable
    ) {
        return new KDeleteFrom(kExecutor, kTable);
    }
    
    protected static KDeleteFrom getInstance(
        final KExecutor kExecutor,
        final KQueryDeleteData kQueryDeleteData,
        final KTable kTable
    ) {
        return new KDeleteFrom(kExecutor, kQueryDeleteData, kTable);
    }
    
    public KReturningDelete returning(
        final KColumnAllowedToReturning... kColumnsAllowedToReturning
    ) {
        return KReturningDelete.getInstance(this.k, this.kQueryDeleteData, kColumnsAllowedToReturning);
    }
    
    public KUsing using(
        final KTable kTable
    ) {
        return KUsing.getInstance(this.k, this.kQueryDeleteData, kTable);
    }
    
    public KUsing using(
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        return KUsing.getInstance(this.k, this.kQueryDeleteData, new KTable(kRaw.content, new KQueryData(kRaw.params)));
    }
    
    public KUsing using(
        final KCommonTableExpressionFilled kCommonTableExpressionFilled
    ) {
        return KUsing.getInstance(this.k, this.kQueryDeleteData, new KTable(null, kCommonTableExpressionFilled.name, kCommonTableExpressionFilled.alias));
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
        
        final KCondition kCondition = new KCondition(kRaw.content, kRaw.params);
        
        return KWhereDelete.getInstance(this.k, this.kQueryDeleteData, kCondition);
    }
    
    public int execute() {
        return super.executeSingle();
    }
    
    private void process(
        final KTable kTable
    ) {
        if (kTable == null) {
            throw KExceptionHelper.internalServerError("The 'kTable' param is required"); 
        }
        
        if (kTable.isRoot) {
            this.kQueryDeleteData.kNodes.add(KNode.getInstance(kTable.getKRowClass(), kTable.alias));
        }
        
        this.kQueryDeleteData.sb.append(kQueryDeleteData.sb.length() > 0 ? " " : "").append("DELETE FROM ").append(kTable.toSql(true));
        
        if (kTable.kQueryData != null) {
            this.kQueryDeleteData.params.addAll(kTable.kQueryData.params);
        }
    }
}
