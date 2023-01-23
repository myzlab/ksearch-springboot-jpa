package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToReturning;
import com.myzlab.k.helper.KExceptionHelper;

public class KReturningDelete extends KQueryDelete {

    private KReturningDelete() {
        super();
    }
    
    private KReturningDelete(
        final KExecutor kExecutor,
        final KQueryDeleteData kQueryDeleteData,
        final KColumnAllowedToReturning... kColumnsAllowedToReturning
    ) {
        super(kQueryDeleteData, kExecutor);
        
        for (final KColumnAllowedToReturning kColumnAllowedToReturning : kColumnsAllowedToReturning) {
            if (kColumnAllowedToReturning == null) {
                throw KExceptionHelper.internalServerError("'kColumnAllowedToReturning' cannot be null"); 
            }
            
            this.kQueryDeleteData.kBaseColumns.add(kColumnAllowedToReturning.getKBaseColumn());
        }
        
        this.process(kColumnsAllowedToReturning);
    }
    
    protected static KReturningDelete getInstance(
        final KExecutor kExecutor,
        final KQueryDeleteData kQueryDeleteData,
        final KColumnAllowedToReturning... kColumnsAllowedToReturning
    ) {
        return new KReturningDelete(kExecutor, kQueryDeleteData, kColumnsAllowedToReturning);
    }
    
    private void process(
        final KColumnAllowedToReturning... kColumnsAllowedToReturning
    ) {
        if (kColumnsAllowedToReturning == null || kColumnsAllowedToReturning.length == 0) {
            throw KExceptionHelper.internalServerError("The 'kBaseColums' param is required"); 
        }
        
        this.kQueryDeleteData.sb.append(" RETURNING ");
        
        for (int i = 0; i < kColumnsAllowedToReturning.length; i++) {
            final KColumnAllowedToReturning kColumnAllowedToReturning = kColumnsAllowedToReturning[i];
            
            if (i > 0) {
                this.kQueryDeleteData.sb.append(", ");
            }
            
            this.kQueryDeleteData.params.addAll(kColumnAllowedToReturning.getParams());
            this.kQueryDeleteData.sb.append(kColumnAllowedToReturning.getSqlToReturning());
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