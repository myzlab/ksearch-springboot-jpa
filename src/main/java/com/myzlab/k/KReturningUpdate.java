package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToReturning;
import com.myzlab.k.helper.KExceptionHelper;

public class KReturningUpdate extends KQueryUpdate {

    private KReturningUpdate() {
        super();
    }
    
    private KReturningUpdate(
        final KExecutor kExecutor,
        final KQueryUpdateData kQueryUpdateData,
        final KColumnAllowedToReturning... kColumnsAllowedToReturning
    ) {
        super(kQueryUpdateData, kExecutor);
        
        for (final KColumnAllowedToReturning kColumnAllowedToReturning : kColumnsAllowedToReturning) {
            if (kColumnAllowedToReturning == null) {
                throw KExceptionHelper.internalServerError("'kColumnAllowedToReturning' cannot be null"); 
            }
            
            this.kQueryUpdateData.kBaseColumns.add(kColumnAllowedToReturning.getKBaseColumn());
        }
        
        this.process(kColumnsAllowedToReturning);
    }
    
    protected static KReturningUpdate getInstance(
        final KExecutor kExecutor,
        final KQueryUpdateData kQueryUpdateData,
        final KColumnAllowedToReturning... kColumnsAllowedToReturning
    ) {
        return new KReturningUpdate(kExecutor, kQueryUpdateData, kColumnsAllowedToReturning);
    }
    
    private void process(
        final KColumnAllowedToReturning... kColumnsAllowedToReturning
    ) {
        if (kColumnsAllowedToReturning == null || kColumnsAllowedToReturning.length == 0) {
            throw KExceptionHelper.internalServerError("The 'kBaseColums' param is required"); 
        }
        
        this.kQueryUpdateData.sb.append(" RETURNING ");
        
        for (int i = 0; i < kColumnsAllowedToReturning.length; i++) {
            final KColumnAllowedToReturning kColumnAllowedToReturning = kColumnsAllowedToReturning[i];
            
            if (i > 0) {
                this.kQueryUpdateData.sb.append(", ");
            }
            
            this.kQueryUpdateData.params.addAll(kColumnAllowedToReturning.getParams());
            this.kQueryUpdateData.sb.append(kColumnAllowedToReturning.getSqlToReturning());
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