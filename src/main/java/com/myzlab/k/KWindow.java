package com.myzlab.k;

import static com.myzlab.k.KFunction.*;
import com.myzlab.k.allowed.KColumnAllowedToOrderBy;
import com.myzlab.k.allowed.KQueryAllowedToCombining;
import com.myzlab.k.allowed.KWindowDefinitionAllowedToWindow;
import com.myzlab.k.optional.KOptionalLong;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KWindow extends KQuery implements KQueryAllowedToCombining {
    
    final List<KWindowDefinitionAllowedToWindow> kWindowDefinitionsAllowedToWindow = new ArrayList<>();
    
    private KWindow(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQueryData kQueryData,
        final KWindowDefinitionAllowedToWindow... KWindowDefinitionsAllowedToWindow
    ) {
        super(kQueryData, kExecutor, kSpecialFunctions);
        
        KUtils.assertNotNull(KWindowDefinitionsAllowedToWindow, "KWindowDefinitionsAllowedToWindow");
        
        this.kWindowDefinitionsAllowedToWindow.addAll(Arrays.asList(KWindowDefinitionsAllowedToWindow));
    }
    
    protected static KWindow getInstance(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQueryData kQueryData,
        final KWindowDefinitionAllowedToWindow... KWindowDefinitionsAllowedToWindow
    ) {
        return new KWindow(kExecutor, kSpecialFunctions, kQueryData, KWindowDefinitionsAllowedToWindow);
    }
    
    public KTable as(
        final String alias
    ) {
        return table(this, alias);
    }
    
    public KTable as(
        final String alias,
        final String... names
    ) {
        return table(this, alias, tuple(names));
    }
    
    public KWindow window(
        final KWindowDefinitionAllowedToWindow... KWindowDefinitionsAllowedToWindow
    ) {
        KUtils.assertNotNull(KWindowDefinitionsAllowedToWindow, "KWindowDefinitionsAllowedToWindow");
        
        this.kWindowDefinitionsAllowedToWindow.addAll(Arrays.asList(KWindowDefinitionsAllowedToWindow));
        
        return this;
    }

    public KCombining union(
        final KQueryAllowedToCombining kQueryAllowedToCombining
    ) {
        final KQueryData clone = this.kQueryData.cloneMe();
        
        this.buildWindow(clone);
        
        return KCombining.getInstance(this.k, this.kSpecialFunctions, clone, kQueryAllowedToCombining, "UNION", false);
    }
    
    public KCombining unionAll(
        final KQueryAllowedToCombining kQueryAllowedToCombining
    ) {
        final KQueryData clone = this.kQueryData.cloneMe();
        
        this.buildWindow(clone);
        
        return KCombining.getInstance(this.k, this.kSpecialFunctions, clone, kQueryAllowedToCombining, "UNION", true);
    }
    
    public KCombining intersect(
        final KQueryAllowedToCombining kQueryAllowedToCombining
    ) {
        final KQueryData clone = this.kQueryData.cloneMe();
        
        this.buildWindow(clone);
        
        return KCombining.getInstance(this.k, this.kSpecialFunctions, clone, kQueryAllowedToCombining, "INTERSECT", false);
    }
    
    public KCombining intersectAll(
        final KQueryAllowedToCombining kQueryAllowedToCombining
    ) {
        final KQueryData clone = this.kQueryData.cloneMe();
        
        this.buildWindow(clone);
        
        return KCombining.getInstance(this.k, this.kSpecialFunctions, clone, kQueryAllowedToCombining, "INTERSECT", true);
    }
    
    public KCombining except(
        final KQueryAllowedToCombining kQueryAllowedToCombining
    ) {
        final KQueryData clone = this.kQueryData.cloneMe();
        
        this.buildWindow(clone);
        
        return KCombining.getInstance(this.k, this.kSpecialFunctions, clone, kQueryAllowedToCombining, "EXCEPT", false);
    }
    
    public KCombining exceptAll(
        final KQueryAllowedToCombining kQueryAllowedToCombining
    ) {
        final KQueryData clone = this.kQueryData.cloneMe();
        
        this.buildWindow(clone);
        
        return KCombining.getInstance(this.k, this.kSpecialFunctions, clone, kQueryAllowedToCombining, "EXCEPT", true);
    }
    
    public KOrderBy orderBy(
        final KColumnAllowedToOrderBy... kColumnsAllowedToOrderBy
    ) {
        final KQueryData clone = this.kQueryData.cloneMe();
        
        this.buildWindow(clone);
        
        return KOrderBy.getInstance(this.k, this.kSpecialFunctions, clone, kColumnsAllowedToOrderBy);
    }
    
    public KLimit limit(
        final int count
    ) {
        final KQueryData clone = this.kQueryData.cloneMe();
        
        this.buildWindow(clone);
        
        return KLimit.getInstance(this.k, this.kSpecialFunctions, clone, count);
    }
    
    public KLimit limit(
        final long count
    ) {
        final KQueryData clone = this.kQueryData.cloneMe();
        
        this.buildWindow(clone);
        
        return KLimit.getInstance(this.k, this.kSpecialFunctions, clone, count);
    }
    
    public KLimit limit(
        final KOptionalLong kOptionalLong
    ) {
        final KQueryData clone = this.kQueryData.cloneMe();
        
        this.buildWindow(clone);
        
        if (!kOptionalLong.isPresent()) {
            return KLimit.getInstance(this.k, this.kSpecialFunctions, clone);
        }
        
        return KLimit.getInstance(this.k, this.kSpecialFunctions, clone, kOptionalLong.get());
    }
    
    public KOffset offset(
        final int start
    ) {
        final KQueryData clone = this.kQueryData.cloneMe();
        
        this.buildWindow(clone);
        
        return KOffset.getInstance(this.k, this.kSpecialFunctions, clone, start);
    }
    
    public KOffset offset(
        final long start
    ) {
        final KQueryData clone = this.kQueryData.cloneMe();
        
        this.buildWindow(clone);
        
        return KOffset.getInstance(this.k, this.kSpecialFunctions, clone, start);
    }
    
    public KOffset offset(
        final KOptionalLong kOptionalLong
    ) {
        final KQueryData clone = this.kQueryData.cloneMe();
        
        this.buildWindow(clone);
        
        if (!kOptionalLong.isPresent()) {
            return KOffset.getInstance(this.k, this.kSpecialFunctions, clone);
        }
        
        return KOffset.getInstance(this.k, this.kSpecialFunctions, clone, kOptionalLong.get());
    }
    
    public KFetch fetch(
        final int rowCount
    ) {
        final KQueryData clone = this.kQueryData.cloneMe();
        
        this.buildWindow(clone);
        
        return KFetch.getInstance(this.k, this.kSpecialFunctions, clone, rowCount);
    }
    
    public KFetch fetch(
        final long rowCount
    ) {
        final KQueryData clone = this.kQueryData.cloneMe();
        
        this.buildWindow(clone);
        
        return KFetch.getInstance(this.k, this.kSpecialFunctions, clone, rowCount);
    }
    
    public KFetch fetch(
        final KOptionalLong kOptionalLong
    ) {
        final KQueryData clone = this.kQueryData.cloneMe();
        
        this.buildWindow(clone);
        
        if (!kOptionalLong.isPresent()) {
            return KFetch.getInstance(this.k, this.kSpecialFunctions, clone);
        }
        
        return KFetch.getInstance(this.k, this.kSpecialFunctions, clone, kOptionalLong.get());
    }
    
    private void buildWindow(
        final KQueryData kQueryData
    ) {
        KQueryUtils.buildWindow(
            kQueryData, 
            this.kWindowDefinitionsAllowedToWindow
        );
        
        for (final KSpecialFunction kSpecialFunction : this.kSpecialFunctions) {
            kSpecialFunction.onBuildWindow(this.kWindowDefinitionsAllowedToWindow);
        }
    }
    
    @Override
    public <T> T single(
        final Class<T> clazz
    ) {
        this.buildWindow(this.kQueryData);
        
        return super.single(clazz);
    }
    
    @Override
    public <T extends KRow> KCollection<T> multiple(
        final Class<T> clazz
    ) {
        this.buildWindow(this.kQueryData);
        
        return super.multiple(clazz);
    }
    
    @Override
    public KQueryData generateSubQueryData() {
        final KQueryData newKQueryData = this.kQueryData.cloneMe();
        
        KQueryUtils.buildWindow(
            newKQueryData, 
            this.kWindowDefinitionsAllowedToWindow
        );
        
        return newKQueryData;
    }
}
