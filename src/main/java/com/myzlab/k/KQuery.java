package com.myzlab.k;

import com.myzlab.k.helper.KExceptionHelper;
import java.util.ArrayList;
import java.util.List;
import javax.persistence.NoResultException;
import javax.persistence.NonUniqueResultException;
import javax.persistence.Query;

public abstract class KQuery extends KGenericQuery {
    
    protected KExecutor k;
    protected KQueryData kQueryData;
    protected final List<KSpecialFunction> kSpecialFunctions;

    protected KQuery() {
        this.kQueryData = new KQueryData();
        this.kSpecialFunctions = new ArrayList<>();
    }
    
    protected KQuery(
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions
    ) {
        this.k = kExecutor;
        this.kQueryData = new KQueryData();
        this.kSpecialFunctions = kSpecialFunctions;
    }
    
    public KQuery(
        final KQueryData kQueryData,
        final KExecutor kExecutor,
        final List<KSpecialFunction> kSpecialFunctions
    ) {
        this.k = kExecutor;
        this.kQueryData = kQueryData;
        this.kSpecialFunctions = kSpecialFunctions;
    }
    
    private <T extends KRow> T singleMappingKRow(
        final Class<T> clazz
    ) {
        final List<String[]> paths = KQueryUtils.getPaths(this.kQueryData, clazz);
        
        final Query query = k.getEntityManager().createNativeQuery(this.kQueryData.sb.toString());
        
        int i = 1;
        
        for (final Object param : this.kQueryData.params) {
            query.setParameter(i++, param);
        }
        
        Object object = null;
        
        try {
            object = query.getSingleResult();
            
            if (object == null) {
                return KQueryUtils.mapObject(
                    this.kQueryData, new Object[] {
                        null
                    }, 
                    paths,
                    clazz
                );
            }

            return KQueryUtils.mapObject(this.kQueryData, (Object[]) object, paths, clazz);
        } catch (NoResultException | NonUniqueResultException e) {
            return KQueryUtils.getKRowNull(clazz);
        } catch (ClassCastException e) {
            return KQueryUtils.mapObject(this.kQueryData, new Object[]{
                object
            }, paths, clazz);
        } catch (Exception e) {
            e.printStackTrace();

            return null;
        }
    }
    
    private <T> T singleMappingSingleType() {
        final Query query = k.getEntityManager().createNativeQuery(this.kQueryData.sb.toString());
        
        int i = 1;
        
        for (final Object param : this.kQueryData.params) {
            query.setParameter(i++, param);
        }
        
        try {
            return (T) query.getSingleResult();
        } catch (Exception e) {
            e.printStackTrace();

            return null;
        }
    }
    
    public KRow single() {
        return this.single(KRow.class);
    }
    
    public <T> T single(
        final Class<T> clazz
    ) {
//        System.out.println(this.kQueryData.sb.toString());
//        System.out.println(this.kQueryData.params);
//        System.out.println(this.kQueryData.kBaseColums);
            
        if (k == null || k.getEntityManager() == null) {
            System.err.println("EntityManager no provided to KSearch!");
            
            return null;
        }
        
        if (clazz.getSuperclass().equals(KRow.class) || clazz.equals(KRow.class)) {
            return (T) this.singleMappingKRow((Class<? extends KRow>) clazz);
        }
        
        if (this.kQueryData.kBaseColumns.size() > 1) {
            throw KExceptionHelper.internalServerError("Only a single column is allowed in the 'SELECT clause for the requested mapping type");
        }
        
        return this.singleMappingSingleType();
    }
    
    public <T extends KRow> KCollection<T> multiple(
        final Class<T> clazz
    ) {
        return KQueryUtils.multipleMapping(this.k, this.kSpecialFunctions, this.kQueryData, clazz);
    }
    
    public KCollection<KRow> multiple() {
        return this.multiple(KRow.class);
    }
    
    
//    private KRow mapObject(
//        final ResultSet resultSet
//    ) throws SQLException {
//        final Object[] o = new Object[this.kQueryData.kBaseColumns.size()];
//        final Map<String, Integer> ref = new HashMap<>();
//
//        for (int i = 0; i < this.kQueryData.kBaseColumns.size(); i++) {
//            final Object v = resultSet.getObject(i + 1);
//            final KBaseColumn kBaseColumn = this.kQueryData.kBaseColumns.get(i);
//            
//            if (kBaseColumn == null) {
//                throw KExceptionHelper.internalServerError("The 'kBaseColumn' is required"); 
//            }
//            
//            o[i] = v;
//            KUtils.fillRef(ref, kBaseColumn, i);
//        }
//
//        return new KRow(o, ref);
//    }
    
    @Override
    protected KQueryGenericData generateSubQueryData() {
        return this.kQueryData.cloneMe();
    }
}
