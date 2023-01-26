package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToReturning;
import com.myzlab.k.helper.KExceptionHelper;

public class KReturningInsertInto extends KQueryInsertInto {

    private KReturningInsertInto() {
        super();
    }
    
    private KReturningInsertInto(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData,
        final KColumnAllowedToReturning... kColumnsAllowedToReturning
    ) {
        super(kQueryInsertIntoData, kExecutor);
        
        for (final KColumnAllowedToReturning kColumnAllowedToReturning : kColumnsAllowedToReturning) {
            if (kColumnAllowedToReturning == null) {
                throw KExceptionHelper.internalServerError("'kColumnAllowedToReturning' cannot be null"); 
            }
            
            this.kQueryInsertIntoData.kBaseColumns.add(kColumnAllowedToReturning.getKBaseColumn());
        }
        
        this.process(kColumnsAllowedToReturning);
    }
    
    protected static KReturningInsertInto getInstance(
        final KExecutor kExecutor,
        final KQueryInsertIntoData kQueryInsertIntoData,
        final KColumnAllowedToReturning... kColumnsAllowedToReturning
    ) {
        return new KReturningInsertInto(kExecutor, kQueryInsertIntoData, kColumnsAllowedToReturning);
    }
    
    private void process(
        final KColumnAllowedToReturning... kColumnsAllowedToReturning
    ) {
        if (kColumnsAllowedToReturning == null || kColumnsAllowedToReturning.length == 0) {
            throw KExceptionHelper.internalServerError("The 'kBaseColums' param is required"); 
        }
        
        this.kQueryInsertIntoData.sb.append(" RETURNING ");
        
        for (int i = 0; i < kColumnsAllowedToReturning.length; i++) {
            final KColumnAllowedToReturning kColumnAllowedToReturning = kColumnsAllowedToReturning[i];
            
            if (i > 0) {
                this.kQueryInsertIntoData.sb.append(", ");
            }
            
            this.kQueryInsertIntoData.params.addAll(kColumnAllowedToReturning.getParams());
            this.kQueryInsertIntoData.sb.append(kColumnAllowedToReturning.getSqlToReturning());
        }
    }
    
    public <T extends KRow> KCollection<T> execute(
        final Class<T> clazz
    ) {
        return super.executeMapping(clazz);
    }
    
    public KCollection<KRow> execute() {
        return super.executeMapping();
    }
}