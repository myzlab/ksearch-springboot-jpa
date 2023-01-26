package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToGroupBy;
import com.myzlab.k.allowed.KColumnAllowedToOrderBy;
import com.myzlab.k.allowed.KQueryAllowedToCombining;
import com.myzlab.k.allowed.KWindowDefinitionAllowedToWindow;
import com.myzlab.k.helper.KExceptionHelper;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.persistence.Query;
import org.postgresql.jdbc.PgArray;
import org.postgresql.jdbc.PgSQLXML;
import org.postgresql.util.PGobject;

public class KQueryUtils {
    
    protected static void fillRef(
        final Map<String, Integer> ref,
        final KBaseColumn kBaseColumn,
        final int i
    ) {
        final String nameToRef;
            
        if (kBaseColumn instanceof KAliasedColumn) {
            nameToRef = ((KAliasedColumn) kBaseColumn).alias;
        } else if (kBaseColumn instanceof KTableColumn) {
            nameToRef = kBaseColumn.name;
        } else {
            nameToRef = KUtils.reverseParams(kBaseColumn);
        }
        
        if (ref.containsKey(nameToRef)) {
            throw KExceptionHelper.internalServerError("'Ref' value repeated. Please check values of 'SELECT' Clause. Repeated Value: ['" + nameToRef + "']");
        }

        ref.put(nameToRef, i);
    }
    
    private static List<KPath> getNewPathsByAliasSource(
        final List<KPath> kPaths,
        final String aliasSource,
        final String aliasTarget,
        final String path
    ) {
        final List<KPath> newKPaths = new ArrayList<>();
        
        for (final KPath kPath : kPaths) {
            if (aliasSource.equals(kPath.aliasTarget)) {
                newKPaths.add(KPath.getInstance(kPath.aliasSource, aliasTarget, kPath.path + "," + path));
            }
        }
        
        return newKPaths;
    }
    
    private static List<KPath> getNewPathsByAliasTarget(
        final List<KPath> kPaths,
        final String aliasSource,
        final String aliasTarget,
        final String path
    ) {
        final List<KPath> newKPaths = new ArrayList<>();
        
        for (final KPath kPath : kPaths) {
            if (kPath.aliasSource.equals(aliasTarget)) {
                newKPaths.add(KPath.getInstance(aliasSource, kPath.aliasTarget, path + "," + kPath.path));
            }
        }
        
        return newKPaths;
    }
    
    private static <T extends KRow> String getCurrentAlias(
        final List<KNode> kNodes,
        final Class<T> clazz
    ) {
        for (final KNode kNode : kNodes) {
            if (kNode.source.equals(clazz)) {
                return kNode.aliasSource;
            }
        }
        
        return null;
    }
    
    protected static <T extends KRow> List<String[]> getPaths(
        final KQueryGenericData kQueryGenericData,
        final Class<T> clazz
    ) {
        
        if (clazz.equals(KRow.class)) {
            return new ArrayList<>();
        }
        
        final List<KPath> kPaths = new ArrayList<>();
        
        for (final KEdge kEdge : kQueryGenericData.kEdges) {
            final KNode kNodeSource = KNode.getInstance(kEdge.source, kEdge.aliasSource);
            final KNode kNodeTarget = KNode.getInstance(kEdge.target, kEdge.aliasTarget);
            
            if (!kQueryGenericData.kNodes.contains(kNodeSource)) {
                kQueryGenericData.kNodes.add(kNodeSource);
            }
            
            if (!kQueryGenericData.kNodes.contains(kNodeTarget)) {
                kQueryGenericData.kNodes.add(kNodeTarget);
            }
            
            final List<KPath> newKPaths = new ArrayList<>();
            
            newKPaths.add(KPath.getInstance(kEdge.aliasSource, kEdge.aliasTarget, kEdge.path));
            
            newKPaths.addAll(getNewPathsByAliasSource(kPaths, kEdge.aliasSource, kEdge.aliasTarget, kEdge.path));
            newKPaths.addAll(getNewPathsByAliasTarget(kPaths, kEdge.aliasSource, kEdge.aliasTarget, kEdge.path));
            
            kPaths.addAll(newKPaths);
        }
        
        final String currentAlias = getCurrentAlias(kQueryGenericData.kNodes, clazz);
        
        final List<String[]> paths = new ArrayList<>();
        
        if (currentAlias == null) {
            return paths;
        }
        
        final Map<String, String[]> allPaths = new HashMap<>();
        
        allPaths.put(currentAlias, "*".split(","));
        
        for (final KPath kPath : kPaths) {
            if (kPath.aliasSource.equals(currentAlias)) {
                allPaths.put(kPath.aliasTarget, (kPath.path + ",*").split(","));
            }
        }
        
        for (int i = 0; i < kQueryGenericData.kBaseColumns.size(); i++) {
            final KBaseColumn kBaseColumn = kQueryGenericData.kBaseColumns.get(i);
            
            if (kBaseColumn.name == null || kBaseColumn.kTable == null || kBaseColumn.type == null || kBaseColumn.kTable.alias == null) {
                paths.add(null);
                
                continue;
            }
            
            paths.add(allPaths.get(kBaseColumn.kTable.alias));
        }
        
        return paths;
    }
    
    protected static <T extends KRow> void mapColumn(
        final T t,
        final Object v,
        final String[] path,
        final KBaseColumn kBaseColumn
    ) {
        if (path == null) {
            return;
        }
        
        Object current = t;

        for (final String w : path) {
            if (w.equals("*")) {
                try {
                    final Method methodSet = current.getClass().getMethod(
                        KSearchNameHelper.generateSetName(kBaseColumn.name),
                        kBaseColumn.type
                    );
                    
                    methodSet.invoke(current, getValueByClass(v, methodSet.getParameterTypes()[0]));
                } catch (Exception e) {
                    throw KExceptionHelper.internalServerError(e.getMessage());
                }
            } else {
                try {
                    final Method methodGet = current.getClass().getMethod(
                        KSearchNameHelper.generateGetName(w)
                    );

                    Object internalObject = methodGet.invoke(current);

                    if (internalObject == null) {
                        try {
                            internalObject = methodGet.getReturnType().newInstance();

                            final Method methodSet = current.getClass().getMethod(
                                KSearchNameHelper.generateSetName(w),
                                methodGet.getReturnType()
                            );

                            methodSet.invoke(current, internalObject);
                        } catch (Exception e) {
                            throw KExceptionHelper.internalServerError(e.getMessage());
                        }
                    }

                    current = internalObject;
                } catch (Exception e) {
                    throw KExceptionHelper.internalServerError(e.getMessage());
                }
            }
        }
    }
    
    private static Object castCustomDBValuesToJava(
        final Object v
    ) {
        try {
            if (v instanceof PgArray) {
                return ((PgArray) v).getArray();
            } else if (v instanceof PGobject) {
                final PGobject pGobject = (PGobject) v;
                
                if (pGobject.getType().equals("json")) {
                    return pGobject.getValue();
                }
                
                if (pGobject.getType().equals("jsonb")) {
                    return pGobject.getValue();
                }
            } else if (v instanceof PgSQLXML) {
                return ((PgSQLXML) v).getString();
            }

            return v;
        } catch (Exception e) {
            e.printStackTrace();
            
            return v;
        }
    }
    
    protected static <T extends KRow> T mapObject(
        final KQueryGenericData kQueryGenericData,
        final Object[] resultSet,
        final List<String[]> paths,
        final Class<T> clazz
    ) {
        
        final T t;
        
        try {
            t = (T) clazz.newInstance();  
        } catch (Exception e) {
            throw KExceptionHelper.internalServerError(e.getMessage());
        }
        
        final Object[] o = new Object[kQueryGenericData.kBaseColumns.size()];
        final Map<String, Integer> ref = new HashMap<>();

        for (int i = 0; i < kQueryGenericData.kBaseColumns.size(); i++) {
            final Object v_ = resultSet[i];
            final KBaseColumn kBaseColumn = kQueryGenericData.kBaseColumns.get(i);
            
            if (kBaseColumn == null) {
                throw KExceptionHelper.internalServerError("The 'kBaseColumn' is required"); 
            }
            
            final Object v = KQueryUtils.castCustomDBValuesToJava(v_);
            
            KQueryUtils.mapColumn(
                t,
                v,
                paths.isEmpty() ? null : paths.get(i),
                kBaseColumn
            );
            
            o[i] = v;
            KQueryUtils.fillRef(ref, kBaseColumn, i);
        }
        
        t.o = o;
        t.ref = ref;

        return t;
    }
    
    protected static <T extends KRow> KCollection<T> multipleMapping(
        final KExecutor k,
        final List<KSpecialFunction> kSpecialFunctions,
        final KQueryGenericData kQueryGenericData,
        final Class<T> clazz
    ) {
//        System.out.println(kQueryGenericData.sb.toString());
//        System.out.println(kQueryGenericData.params);
//        System.out.println(kQueryGenericData.kEdges);
//        System.out.println(kQueryGenericData.kNodes);
        
        if (k == null || k.getEntityManager() == null) {
             System.err.println("EntityManager no provided to KSearch!");
            
            return null;
        }
        
        final List<String[]> paths = KQueryUtils.getPaths(kQueryGenericData, clazz);
        
        final Query query = k.getEntityManager().createNativeQuery(kQueryGenericData.sb.toString());
        
        int i = 1;
        
        for (final Object param : kQueryGenericData.params) {
            query.setParameter(i++, param);
        }
        
        final List<Object> objects = query.getResultList();
        final List<T> list = new ArrayList<>();
        
        if (objects == null || objects.isEmpty()) {
            return new KCollection(list);
        }
        
        final boolean isArrayObject = objects.get(0) instanceof Object[];
        
        if (isArrayObject) {
            for (Object object : objects) {
                list.add(mapObject(kQueryGenericData, (Object[]) object, paths, clazz));
            }
        } else {
            for (Object object : objects) {
                list.add(mapObject(kQueryGenericData, new Object[] {
                    object
                }, paths, clazz));
            }
        }
        
        final KCollection kCollection = new KCollection<>(list);
        
        for (KSpecialFunction kSpecialFunction : kSpecialFunctions) {
            kSpecialFunction.executeOnMultipleMapping(k, kCollection);
        }
        
        return kCollection;
    }
    
    protected static int executeSingle(
        final KExecutor k,
        final KQueryGenericData kQueryGenericData
    ) {
//        System.out.println(kQueryGenericData.sb.toString());
//        System.out.println(kQueryGenericData.params);
        
        if (k == null || k.getEntityManager() == null) {
            System.err.println("EntityManager no provided to KSearch!");
            
            return -1;
        }
        
        final Query query = k.getEntityManager().createNativeQuery(kQueryGenericData.sb.toString());
        
        int i = 1;
        
        for (final Object param : kQueryGenericData.params) {
            query.setParameter(i++, param);
        }
        
        return query.executeUpdate();
    }
    
    protected static void processWith(
        final KQueryData kQueryData,
        final boolean recursive,
        final KCommonTableExpressionFilled... kCommonTableExpressionsFilled
    ) {
        KUtils.assertNotNullNotEmpty(kCommonTableExpressionsFilled, "kCommonTableExpressionsFilled", false);
        
        kQueryData.sb.append("WITH ").append(recursive ? "RECURSIVE " : "");
        
        for (int i = 0; i < kCommonTableExpressionsFilled.length; i++) {
            final KCommonTableExpressionFilled kCommonTableExpressionFilled = kCommonTableExpressionsFilled[i];
            
            if (i > 0) {
                kQueryData.sb.append(", ");
            }
            
            kQueryData.sb.append(kCommonTableExpressionFilled.name);
            
            final String[] columns = kCommonTableExpressionFilled.columns;
            
            if (columns != null && columns.length > 0) {
                kQueryData.sb.append(" (");

                for (int j = 0; j < columns.length; j++) {
                    if (j > 0) {
                        kQueryData.sb.append(", ");
                    }

                    kQueryData.sb.append(columns[j]);
                }

                kQueryData.sb.append(")");
            }
            
            kQueryData.sb.append(" AS ");
            
            if (kCommonTableExpressionFilled.kValues != null) {
                KQueryUtils.processValuesWith(kQueryData, kCommonTableExpressionFilled.kValues.values);

                return;
            }
            
            KQueryUtils.processKQueryWith(kQueryData, kCommonTableExpressionFilled.kGenericQuery);
        }
    }
    
    protected static void processValuesWith(
        final KQueryData kQueryData,
        final List<List<Object>> allValues
    ) {
        kQueryData.sb.append("(VALUES ");
            
        for (int i = 0; i < allValues.size(); i++) {
            final List<Object> values = allValues.get(i);
            
            if (i > 0) {
                kQueryData.sb.append(", ");
            }

            kQueryData.sb.append("(");

            for (int j = 0; j < values.size(); j++) {
                if (j > 0) {
                    kQueryData.sb.append(", ");
                }
                
                if (values.get(j) instanceof KRaw) {
                    kQueryData.sb.append(((KRaw) values.get(j)).content);
                } else {
                    kQueryData.sb.append("?");
                    kQueryData.params.add(values.get(j));
                }
                
            }

            kQueryData.sb.append(")");
        }

        kQueryData.sb.append(")");
    }
    
    protected static void processKQueryWith(
        final KQueryData kQueryData,
        final KGenericQuery kGenericQuery
    ) {
        final KQueryGenericData subQuery = kGenericQuery.generateSubQueryData();
        
        kQueryData.sb.append("(").append(subQuery.sb).append(")");
        
        kQueryData.params.addAll(subQuery.params);
    }
    
    protected static void processSelectDistinctOn(
        final KQueryData kQueryData,
        final KColumn kColumn
    ) {
        KUtils.assertNotNull(kColumn, "kColumn");
        
        kQueryData.distinctOn = true;
        kQueryData.params.addAll(kColumn.params);
        kQueryData.sb.append(kQueryData.sb.length() > 0 ? " " : "").append("SELECT DISTINCT ON (").append(kColumn.sb).append(")");
    }
    
    protected static void processSelectDistinctOn(
        final KQueryData kQueryData,
        final int n
    ) {
        KUtils.assertNotNull(n, "n");
        
        kQueryData.distinctOn = true;
        kQueryData.sb.append("SELECT DISTINCT ON (").append(n).append(")");
    }
    
    protected static void processSelect(
        final KQueryData kQueryData,
        final boolean distinct,
        final List<KBaseColumn> kBaseColumns
    ) {
        kQueryData.kBaseColumns.addAll(kBaseColumns);
        
        if (kQueryData.sb.length() > 0 && kQueryData.columnsAdded == 0) {
            kQueryData.sb.append(" ");
        }
        
        if (kQueryData.columnsAdded == 0 && !kQueryData.distinctOn) {
            kQueryData.sb.append("SELECT ").append(distinct ? "DISTINCT " : "");
        }
        
        for (final KBaseColumn kBaseColumn : kBaseColumns) {
            if (kQueryData.columnsAdded > 0) {
                kQueryData.sb.append(", ");
            }
            
            kQueryData.columnsAdded++;
            kQueryData.params.addAll(kBaseColumn.params);
            
            kQueryData.sb.append(kBaseColumn.sb);
        }
    }
    
    protected static void processSelect(
        final KQueryData kQueryData,
        final boolean distinct,
        final KQuery kQuery,
        final String alias
    ) {
        KUtils.assertNotNull(kQuery, "kQuery");
        KUtils.assertNotNull(alias, "alias");
        
        final KQueryGenericData subQuery = kQuery.generateSubQueryData();
        
        kQueryData.kBaseColumns.add(new KColumn(subQuery.sb, subQuery.params, false).as(alias));
        
        if (kQueryData.sb.length() > 0 && kQueryData.columnsAdded == 0) {
            kQueryData.sb.append(" ");
        }
        
        if (kQueryData.columnsAdded == 0 && !kQueryData.distinctOn) {
            kQueryData.sb.append("SELECT ").append(distinct ? "DISTINCT " : "");
        }
        
        if (kQueryData.columnsAdded > 0) {
            kQueryData.sb.append(", ");
        }

        kQueryData.columnsAdded++;
        kQueryData.params.addAll(subQuery.params);

        kQueryData.sb.append("(").append(subQuery.sb).append(") AS ").append(alias);
    }
    
    protected static void processFrom(
        final KQueryData kQueryData,
        final KTable kTable
    ) {
        if (kTable == null) {
            throw KExceptionHelper.internalServerError("The 'kTable' param is required"); 
        }
        
        if (kTable.isRoot) {
            kQueryData.kNodes.add(KNode.getInstance(kTable.getKRowClass(), kTable.alias));
        }
        
        if (kQueryData.tablesAdded == 0) {
            kQueryData.sb.append(" FROM ");
        } else {
            kQueryData.sb.append(", ");
        }
        
        kQueryData.tablesAdded++;
        
        if (kTable.kQueryData != null) {
            kQueryData.params.addAll(kTable.kQueryData.params);
        }
            
        kQueryData.sb.append(kTable.toSql(true));
    }
    
    protected static void processGeneralJoinFrom(
        final KQueryData kQueryData,
        final String joinName,
        final KJoinDefinition kJoinDefinition
    ) {
        KUtils.assertNotNull(kJoinDefinition, "kJoinDefinition");
        
        if (kJoinDefinition.params != null) {
            kQueryData.params.addAll(kJoinDefinition.params);
        }
        
        kQueryData.sb.append(" ").append(joinName).append(" ").append(kJoinDefinition.table).append(" ON (").append(kJoinDefinition.kCondition.sb).append(")");
        kQueryData.params.addAll(kJoinDefinition.kCondition.params);
        
        if (kJoinDefinition.kEdge != null) {
            kQueryData.kEdges.add(kJoinDefinition.kEdge);
        }
    }
    
    protected static void processGeneralJoinFrom(
        final KQueryData kQueryData,
        final String joinName,
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        kQueryData.sb.append(" ").append(joinName).append(" ").append(kRaw.content);
    }
    
    protected static void processCrossJoinFrom(
        final KQueryData kQueryData,
        final KTable kTable
    ) {
        KUtils.assertNotNull(kTable, "kTable");
        
        if (kTable.kQueryData != null) {
            kQueryData.params.addAll(kTable.kQueryData.params);
        }
        
        kQueryData.sb.append(" CROSS JOIN ").append(kTable.toSql(true));
    }
    
    protected static void processCrossJoinFrom(
        final KQueryData kQueryData,
        final KRaw kRaw
    ) {
        KUtils.assertNotNull(kRaw, "kRaw");
        
        kQueryData.sb.append(" CROSS JOIN ").append(kRaw.content);
    }
    
    protected static void buildWhere(
        final KQueryData kQueryData,
        final KCondition kCondition
    ) {
        KUtils.assertNotNull(kCondition, "kCondition");
        
        if (kCondition.emptyCondition) {
            return;
        }
        
        kQueryData.sb.append(" WHERE ").append(kCondition.toSql());
        kQueryData.params.addAll(kCondition.params);
    }
    
    protected static void processGroupBy(
        final KQueryData kQueryData,
        final KColumnAllowedToGroupBy... kColumnsAllowedToGroupBy
    ) {
        if (kColumnsAllowedToGroupBy == null || kColumnsAllowedToGroupBy.length == 0) {
            throw KExceptionHelper.internalServerError("The 'KColumnsAllowedToGroupBy' param is required"); 
        }
        
        kQueryData.sb.append(" GROUP BY ");
        
        for (int i = 0; i < kColumnsAllowedToGroupBy.length; i++) {
            final KColumnAllowedToGroupBy kColumnAllowedToGroupBy = kColumnsAllowedToGroupBy[i];
            
            if (kColumnAllowedToGroupBy == null) {
                throw KExceptionHelper.internalServerError("'kColumnAllowedToGroupBy' is required");
            }
            
            if (i > 0) {
                kQueryData.sb.append(", ");
            }
            
            kQueryData.params.addAll(kColumnAllowedToGroupBy.getParams());
            kQueryData.sb.append(kColumnAllowedToGroupBy.getSqlToGroupBy());
        }
    }
    
    protected static void buildHaving(
        final KQueryData kQueryData,
        final KCondition kCondition
    ) {
        KUtils.assertNotNull(kCondition, "kCondition");
        
        if (kCondition.emptyCondition) {
            return;
        }
        
        kQueryData.sb.append(" HAVING ").append(kCondition.toSql());
        kQueryData.params.addAll(kCondition.params);
    }
    
    protected static void buildWindow(
        final KQueryData kQueryData,
        final List<KWindowDefinitionAllowedToWindow> kWindowDefinitionsAllowedToWindow
    ) {
        kQueryData.sb.append(" WINDOW ");
        
        for (int i = 0; i < kWindowDefinitionsAllowedToWindow.size(); i++) {
            final KWindowDefinitionAllowedToWindow kWindowDefinitionAllowedToWindow = kWindowDefinitionsAllowedToWindow.get(i);

            if (kWindowDefinitionAllowedToWindow == null) {
                throw KExceptionHelper.internalServerError("'kWindowDefinitionAllowedToWindow' is required");
            }
            
            if (i > 0) {
                kQueryData.sb.append(", ");
            }
            
            kQueryData.sb.append("\"").append(kWindowDefinitionAllowedToWindow.getName()).append("\" AS (").append(kWindowDefinitionAllowedToWindow.getSql()).append(")");
            kQueryData.params.addAll(kWindowDefinitionAllowedToWindow.getParams());
        }
    }
    
    protected static void processCombining(
        final KQueryData kQueryData,
        final KQueryAllowedToCombining kQueryAllowedToCombining,
        final String function,
        final boolean all
    ) {
        KUtils.assertNotNull(kQueryAllowedToCombining, "kQueryAllowedToCombining");
        
        final KQueryData subQuery = kQueryAllowedToCombining.generateSubQueryData();
        
        kQueryData.params.addAll(subQuery.params);
        
        kQueryData.sb.append(" ").append(function).append(all ? " ALL" : "").append(" (").append(subQuery.sb).append(")");
    }
    
    protected static void processOrderBy(
        final KQueryData kQueryData,
        final KColumnAllowedToOrderBy... kColumnsAllowedToOrderBy
    ) {
        KUtils.assertNotNullNotEmpty(kColumnsAllowedToOrderBy, "kColumnsAllowedToOrderBy", false);
        
        final List<KColumnAllowedToOrderBy> kColumnsAllowedToOrderByList =
            Arrays.asList(kColumnsAllowedToOrderBy)
                .stream()
                .filter(s-> s.getSqlToOrderBy() != null)
                .collect(Collectors.toList());
        
        if (kColumnsAllowedToOrderByList.isEmpty()) {
            return;
        }
        
        kQueryData.sb.append(" ORDER BY ");
        
        for (int i = 0; i < kColumnsAllowedToOrderByList.size(); i++) {
            final KColumnAllowedToOrderBy kColumnAllowedToOrderBy = kColumnsAllowedToOrderByList.get(i);
            
            if (i > 0) {
                kQueryData.sb.append(", ");
            }
            
            kQueryData.params.addAll(kColumnAllowedToOrderBy.getParams());
            kQueryData.sb.append(kColumnAllowedToOrderBy.getSqlToOrderBy());
        }
    }
    
    protected static void processLimit(
        final KQueryData kQueryData,
        final long count
    ) {
        kQueryData.sb.append(" LIMIT ").append(count);
    }
    
    protected static void processOffset(
        final KQueryData kQueryData,
        final long start
    ) {
        kQueryData.sb.append(" OFFSET ").append(start);
    }
    
    protected static void processFetch(
        final KQueryData kQueryData,
        final long rowCount
    ) {
        
        kQueryData.sb.append(" FETCH FIRST ");
        
        if (rowCount > 1L) {
            kQueryData.sb.append(rowCount).append(" ");
        }
        
        kQueryData.sb.append("ROW");
        
        if (rowCount > 1L) {
            kQueryData.sb.append("S");
        }
        
        kQueryData.sb.append(" ONLY");
    }
    
    private static <T> T getValueByClass(
        final Object v,
        final Class<T> clazz
    ) {
        if (v == null) {
            return null;
        }
        
        if (clazz == UUID.class) {
            if (v instanceof String) {
                return (T) UUID.fromString((String) v);
            }
            
            return (T) v;
        }
        
        if (clazz == Long.class) {
            if (v instanceof BigInteger) {
                return (T) new Long(((BigInteger) v).longValue());
            }

            return (T) v;
        }
        
        if (clazz == Integer.class) {
            if (v instanceof Short) {
                return (T) new Integer(((Short) v).intValue());
            }

            if (v instanceof BigInteger) {
                return (T) new Long(((BigInteger) v).longValue());
            }
        
            return (T) v;
        }
        
        if (clazz == Double.class) {
            if (v instanceof BigDecimal) {
                return (T) new Double(((BigDecimal) v).doubleValue());
            }
            
            return (T) v;
        }
        
        if (clazz == LocalDateTime.class) {
            if (v instanceof Timestamp) {
                return (T) ((Timestamp) v).toLocalDateTime();
            }
            
            return (T) v;
        }
        
        if (clazz == LocalDate.class) {
            if (v instanceof Date) {
                return (T) ((Date) v).toLocalDate();
            }
            
            return (T) v;
        }
        
        return (T) v;
    }
}
