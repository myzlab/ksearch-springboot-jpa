package com.myzlab.k;

import java.util.ArrayList;

public class KSet extends KQuery {
    
    private KSet(
        final KExecutor kExecutor
    ) {
        super(kExecutor, new ArrayList<>());
    }
    
    protected static KSet getInstance(
        final KExecutor kExecutor
    ) {
        final KSet kSet = new KSet(kExecutor);
        
        kSet.process();
        
        return kSet;
    }
    
    public  void similarityThreshold(
        final Double similarityThreshold
    ) {
        similarityThreshold(similarityThreshold.floatValue());
    }
    
    public void similarityThreshold(
        final Float similarityThreshold
    ) {
        KUtils.assertNotNull(similarityThreshold, "similarityThreshold");
        
        this.kQueryData.sb.append("pg_trgm.similarity_threshold = ").append(similarityThreshold);
        
        this.executeQuery();
    }
    
    private void process() {
        KQueryUtils.processSet(this.kQueryData);
    }

    @Override
    public KQueryData generateSubQueryData() {
        return this.kQueryData.cloneMe();
    }
    
}
