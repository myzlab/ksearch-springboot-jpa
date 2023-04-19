package com.myzlab.k;

import com.myzlab.k.functions.KRowFunction;
import com.myzlab.k.helper.KExceptionHelper;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.json.JSONArray;
import org.springframework.http.ResponseEntity;

public class KCollection<T extends KRow> {

    final List<T> list;
    private final Map<String, Object> metadata;
    private final Class<T> type;

    public KCollection(
        final Class<T> type,
        final List<T> list
    ) {
        super();
        
        this.type = type;
        this.list = list;
        this.metadata = new HashMap<>();
    }
    
    public KCollection(
        final Class<T> type,
        final List<T> list,
        final Map<String, Object> metadata
    ) {
        super();
        
        this.type = type;
        this.list = list;
        this.metadata = new HashMap<>(metadata);
    }
    
    public List<Object> take(
        final String c
    ) {
        final List<Object> taked = new ArrayList<>();
        
        for (final KRow kRow : list) {
            taked.add(kRow.get(c));
        }
        
        return taked;
    }
    
    public List<Object> take(
        final String c,
        final KRowFunction<KRow, Boolean> kRowFunction
    ) {
        final List<Object> taked = new ArrayList<>();
        
        for (final KRow kRow : list) {
            if (kRowFunction.run(kRow)) {
                taked.add(kRow.get(c));
            }
        }
        
        return taked;
    }
    
    public List<Object> take(
        final int n
    ) {
        final List<Object> taked = new ArrayList<>();
        
        for (final KRow kRow : list) {
            taked.add(kRow.get(n));
        }
        
        return taked;
    }
    
    public List<Object> take(
        final int n,
        final KRowFunction<KRow, Boolean> kRowFunction
    ) {
        final List<Object> taked = new ArrayList<>();
        
        for (final KRow kRow : list) {
            if (kRowFunction.run(kRow)) {
                taked.add(kRow.get(n));
            }
        }
        
        return taked;
    }
    
    public List<String> takeString(
        final String c
    ) {
        final List<String> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            taked.add(kRow.getString(c));
        }
        
        return taked;
    }
    
    public List<String> takeString(
        final String c,
        final KRowFunction<KRow, Boolean> kRowFunction
    ) {
        final List<String> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            if (kRowFunction.run(kRow)) {
                taked.add(kRow.getString(c));
            }
        }
        
        return taked;
    }
    
    public List<String> takeString(
        final int n
    ) {
        final List<String> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            taked.add(kRow.getString(n));
        }
        
        return taked;
    }
    
    public List<String> takeString(
        final int n,
        final KRowFunction<KRow, Boolean> kRowFunction
    ) {
        final List<String> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            if (kRowFunction.run(kRow)) {
                taked.add(kRow.getString(n));
            }
        }
        
        return taked;
    }
    
    public List<Character> takeCharacter(
        final String c
    ) {
        final List<Character> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            taked.add(kRow.getCharacter(c));
        }
        
        return taked;
    }
    
    public List<Character> takeCharacter(
        final String c,
        final KRowFunction<KRow, Boolean> kRowFunction
    ) {
        final List<Character> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            if (kRowFunction.run(kRow)) {
                taked.add(kRow.getCharacter(c));
            }
        }
        
        return taked;
    }
    
    public List<Character> takeCharacter(
        final int n
    ) {
        final List<Character> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            taked.add(kRow.getCharacter(n));
        }
        
        return taked;
    }
    
    public List<Character> takeCharacter(
        final int n,
        final KRowFunction<KRow, Boolean> kRowFunction
    ) {
        final List<Character> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            if (kRowFunction.run(kRow)) {
                taked.add(kRow.getCharacter(n));
            }
        }
        
        return taked;
    }
    
    public List<BigDecimal> takeBigDecimal(
        final String c
    ) {
        final List<BigDecimal> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            taked.add(kRow.getBigDecimal(c));
        }
        
        return taked;
    }
    
    public List<BigDecimal> takeBigDecimal(
        final String c,
        final KRowFunction<KRow, Boolean> kRowFunction
    ) {
        final List<BigDecimal> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            if (kRowFunction.run(kRow)) {
                taked.add(kRow.getBigDecimal(c));
            }
        }
        
        return taked;
    }
    
    public List<BigDecimal> takeBigDecimal(
        final int n
    ) {
        final List<BigDecimal> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            taked.add(kRow.getBigDecimal(n));
        }
        
        return taked;
    }
    
    public List<BigDecimal> takeBigDecimal(final int n, final KRowFunction<KRow, Boolean> kRowFunction) {
        final List<BigDecimal> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            if (kRowFunction.run(kRow)) {
                taked.add(kRow.getBigDecimal(n));
            }
        }
        
        return taked;
    }
    
    public List<BigInteger> takeBigInteger(final String c) {
        final List<BigInteger> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            taked.add(kRow.getBigInteger(c));
        }
        
        return taked;
    }
    
    public List<BigInteger> takeBigInteger(final String c, final KRowFunction<KRow, Boolean> kRowFunction) {
        final List<BigInteger> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            if (kRowFunction.run(kRow)) {
                taked.add(kRow.getBigInteger(c));
            }
        }
        
        return taked;
    }
    
    public List<BigInteger> takeBigInteger(final int n) {
        final List<BigInteger> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            taked.add(kRow.getBigInteger(n));
        }
        
        return taked;
    }
    
    public List<BigInteger> takeBigInteger(final int n, final KRowFunction<KRow, Boolean> kRowFunction) {
        final List<BigInteger> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            if (kRowFunction.run(kRow)) {
                taked.add(kRow.getBigInteger(n));
            }
        }
        
        return taked;
    }
    
    public List<Long> takeLong(final String c) {
        final List<Long> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            taked.add(kRow.getLong(c));
        }
        
        return taked;
    }

    public List<Long> takeLong(final String c, final KRowFunction<KRow, Boolean> kRowFunction) {
        final List<Long> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            if (kRowFunction.run(kRow)) {
                taked.add(kRow.getLong(c));
            }
        }
        
        return taked;
    }
    
    public List<Long> takeLong(final int n) {
        final List<Long> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            taked.add(kRow.getLong(n));
        }
        
        return taked;
    }
    
    public List<Long> takeLong(final int n, final KRowFunction<KRow, Boolean> kRowFunction) {
        final List<Long> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            if (kRowFunction.run(kRow)) {
                taked.add(kRow.getLong(n));
            }
        }
        
        return taked;
    }
    
    public List<Integer> takeInteger(final String c) {
        final List<Integer> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            taked.add(kRow.getInteger(c));
        }
        
        return taked;
    }
    
    public List<Integer> takeInteger(final String c, final KRowFunction<KRow, Boolean> kRowFunction) {
        final List<Integer> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            if (kRowFunction.run(kRow)) {
                taked.add(kRow.getInteger(c));
            }
        }
        
        return taked;
    }
    
    public List<Integer> takeInteger(final int n) {
        final List<Integer> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            taked.add(kRow.getInteger(n));
        }
        
        return taked;
    }
    
    public List<Integer> takeInteger(final int n, final KRowFunction<KRow, Boolean> kRowFunction) {
        final List<Integer> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            if (kRowFunction.run(kRow)) {
                taked.add(kRow.getInteger(n));
            }
        }
        
        return taked;
    }
    
    public List<Boolean> takeBoolean(final String c) {
        final List<Boolean> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            taked.add(kRow.getBoolean(c));
        }
        
        return taked;
    }
    
    public List<Boolean> takeBoolean(final String c, final KRowFunction<KRow, Boolean> kRowFunction) {
        final List<Boolean> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            if (kRowFunction.run(kRow)) {
                taked.add(kRow.getBoolean(c));
            }
        }
        
        return taked;
    }
    
    public List<Boolean> takeBoolean(final int n) {
        final List<Boolean> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            taked.add(kRow.getBoolean(n));
        }
        
        return taked;
    }
    
    public List<Boolean> takeBoolean(final int n, final KRowFunction<KRow, Boolean> kRowFunction) {
        final List<Boolean> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            if (kRowFunction.run(kRow)) {
                taked.add(kRow.getBoolean(n));
            }
        }
        
        return taked;
    }
    
    public List<Double> takeDouble(final String c) {
        final List<Double> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            taked.add(kRow.getDouble(c));
        }
        
        return taked;
    }
    
    public List<Double> takeDouble(final String c, final KRowFunction<KRow, Boolean> kRowFunction) {
        final List<Double> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            if (kRowFunction.run(kRow)) {
                taked.add(kRow.getDouble(c));
            }
        }
        
        return taked;
    }
    
    public List<Double> takeDouble(final int n) {
        final List<Double> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            taked.add(kRow.getDouble(n));
        }
        
        return taked;
    }
    
    public List<Double> takeDouble(final int n, final KRowFunction<KRow, Boolean> kRowFunction) {
        final List<Double> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            if (kRowFunction.run(kRow)) {
                taked.add(kRow.getDouble(n));
            }
        }
        
        return taked;
    }
    
    public List<UUID> takeUUID(final String c) {
        final List<UUID> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            taked.add(kRow.getUUID(c));
        }
        
        return taked;
    }

    public List<UUID> takeUUID(final String c, final KRowFunction<KRow, Boolean> kRowFunction) {
        final List<UUID> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            if (kRowFunction.run(kRow)) {
                taked.add(kRow.getUUID(c));
            }
        }
        
        return taked;
    }
    
    public List<UUID> takeUUID(final int n) {
        final List<UUID> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            taked.add(kRow.getUUID(n));
        }
        
        return taked;
    }
    
    public List<UUID> takeUUID(final int n, final KRowFunction<KRow, Boolean> kRowFunction) {
        final List<UUID> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            if (kRowFunction.run(kRow)) {
                taked.add(kRow.getUUID(n));
            }
        }
        
        return taked;
    }
    
    public List<LocalDateTime> takeLocalDateTime(final String c) {
        final List<LocalDateTime> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            taked.add(kRow.getLocalDateTime(c));
        }
        
        return taked;
    }

    public List<LocalDateTime> takeLocalDateTime(final String c, final KRowFunction<KRow, Boolean> kRowFunction) {
        final List<LocalDateTime> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            if (kRowFunction.run(kRow)) {
                taked.add(kRow.getLocalDateTime(c));
            }
        }
        
        return taked;
    }
    
    public List<LocalDateTime> takeLocalDateTime(final int n) {
        final List<LocalDateTime> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            taked.add(kRow.getLocalDateTime(n));
        }
        
        return taked;
    }
    
    public List<LocalDateTime> takeLocalDateTime(final int n, final KRowFunction<KRow, Boolean> kRowFunction) {
        final List<LocalDateTime> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            if (kRowFunction.run(kRow)) {
                taked.add(kRow.getLocalDateTime(n));
            }
        }
        
        return taked;
    }
    
    public List<LocalDate> takeLocalDate(final String c) {
        final List<LocalDate> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            taked.add(kRow.getLocalDate(c));
        }
        
        return taked;
    }

    public List<LocalDate> takeLocalDate(final String c, final KRowFunction<KRow, Boolean> kRowFunction) {
        final List<LocalDate> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            if (kRowFunction.run(kRow)) {
                taked.add(kRow.getLocalDate(c));
            }
        }
        
        return taked;
    }
    
    public List<LocalDate> takeLocalDate(final int n) {
        final List<LocalDate> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            taked.add(kRow.getLocalDate(n));
        }
        
        return taked;
    }
    
    public List<LocalDate> takeLocalDate(final int n, final KRowFunction<KRow, Boolean> kRowFunction) {
        final List<LocalDate> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            if (kRowFunction.run(kRow)) {
                taked.add(kRow.getLocalDate(n));
            }
        }
        
        return taked;
    }
    
    public List<Date> takeDate(final String c) {
        final List<Date> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            taked.add(kRow.getDate(c));
        }
        
        return taked;
    }

    public List<Date> takeDate(final String c, final KRowFunction<KRow, Boolean> kRowFunction) {
        final List<Date> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            if (kRowFunction.run(kRow)) {
                taked.add(kRow.getDate(c));
            }
        }
        
        return taked;
    }
    
    public List<Date> takeDate(final int n) {
        final List<Date> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            taked.add(kRow.getDate(n));
        }
        
        return taked;
    }
    
    public List<Date> takeDate(final int n, final KRowFunction<KRow, Boolean> kRowFunction) {
        final List<Date> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            if (kRowFunction.run(kRow)) {
                taked.add(kRow.getDate(n));
            }
        }
        
        return taked;
    }
    
    public List<Timestamp> takeTimestamp(final String c) {
        final List<Timestamp> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            taked.add(kRow.getTimestamp(c));
        }
        
        return taked;
    }

    public List<Timestamp> takeTimestamp(final String c, final KRowFunction<KRow, Boolean> kRowFunction) {
        final List<Timestamp> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            if (kRowFunction.run(kRow)) {
                taked.add(kRow.getTimestamp(c));
            }
        }
        
        return taked;
    }
    
    public List<Timestamp> takeTimestamp(final int n) {
        final List<Timestamp> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            taked.add(kRow.getTimestamp(n));
        }
        
        return taked;
    }
    
    public List<Timestamp> takeTimestamp(final int n, final KRowFunction<KRow, Boolean> kRowFunction) {
        final List<Timestamp> taked = new ArrayList<>();
        
       for (final KRow kRow : list) {
            if (kRowFunction.run(kRow)) {
                taked.add(kRow.getTimestamp(n));
            }
        }
        
        return taked;
    }
    
    public <T extends KRow> void addProperty(
        final String name,
        final String parentConnectionProperty,
        final String childrenConnectionProperty,
        final KCollection<T> children
    ) {
        this.addProperty(name, parentConnectionProperty, childrenConnectionProperty, children, false);
    }
    
    public <T extends KRow> void addProperty(
        final String name,
        final String parentConnectionProperty,
        final String childrenConnectionProperty,
        final KCollection<T> children,
        final boolean keepChildConnectionProperty
    ) {
        final Map<Object, List<KRow>> groups = new HashMap<>();
        
        for (final KRow kRow : children.list) {
            final Object parentValue = kRow.get(childrenConnectionProperty);
            
            if (parentValue == null) {
                continue;
            }
            
            final KRow clone = kRow.cloneMe();
                
            if (!keepChildConnectionProperty) {
                clone.removeProperty(childrenConnectionProperty);
            }
            
            if (groups.containsKey(parentValue)) {
                groups.get(parentValue).add(clone);
            } else {
                groups.put(parentValue, new ArrayList<KRow>() {{ 
                    add(clone);
                }});
            }
        }
        
        for (final KRow kRow : list) {
            final Object parentValue = kRow.get(parentConnectionProperty);
            
            final List<KRow> childrenGrouped;
            
            if (groups.containsKey(parentValue)) {
                childrenGrouped = groups.get(parentValue);
            } else {
                childrenGrouped = new ArrayList<>();
            }
            
            kRow.addProperty(name, childrenGrouped);
        }
    }
    
    public ResponseEntity buildResponse(
        final String nameItems,
        final String nameMetadata
    ) {
        final List<Map<String, Object>> items = new ArrayList<>();
        
        for (final KRow kRow : list) {
            items.add(kRow.toMap());
        }
        
        if (nameItems == null) {
            return ResponseEntity.ok(items);
        }
        
        final DynamicObject response = DynamicObject.create().add(nameItems, items);
        
        if (!metadata.isEmpty()) {
            response.add(nameMetadata, metadata);
        }
        
        return response.buildResponse();
    }
    
    public List<Map<String, Object>> toList() {
        final List<Map<String, Object>> items = new ArrayList<>();
        
        for (final KRow kRow : list) {
            items.add(kRow.toMap());
        }
        
        return items;
    }
    
    public String toJSON() {
        final List<Map<String, Object>> items = this.toList();
        
        return new JSONArray(items).toString();
    }
    
    public ResponseEntity buildResponse() {
        return buildResponse("items");
    }
    
    public ResponseEntity buildResponse(
        final String nameItems
    ) {
        return buildResponse(nameItems, "metadata");
    }
    
    public int size() {
        return this.list.size();
    }
    
    public boolean isEmpty() {
        return this.list.isEmpty();
    }
    
    public void remove(final int i) {
        this.list.remove(i);
    }
    
    public void removeProperty(final String property) {
//        if (!this.isPresent(property)) {
//            throw KExceptionHelper.internalServerError("The column in KCollection [" + property + "] to be removed does not exist. Ref? [" + ref + "]");
//        }
        
        for (final KRow kRow : list) {
            if (!kRow.isPresent(property)) {
                throw KExceptionHelper.internalServerError("The column in KCollection [" + property + "] to be removed does not exist. Ref? [" + kRow.ref + "]");
            }

            kRow.removeProperty(property);
        }
        
//        final Integer deleted = this.ref.get(column);
//        
//        this.ref.remove(column);
//        
//        for (final Map.Entry<String, Integer> entry : this.ref.entrySet()) {
//            final Integer currentValue = entry.getValue();
//            
//            if (currentValue > deleted) {
//                this.ref.put(entry.getKey(), currentValue - 1);
//            }
//        }
    }
    
//    public Boolean isPresent(final String c) {
//        final Integer n = ref.get(c);
//        
//        if (n == null) {
//            return false;
//        }
//        
//        return true;
//    }
    
    public T get(final int i) {
        return this.list.get(i);
    }
    
    public T getFirst() {
        if (!isEmpty()) {
            return this.get(0);
        }
        
        return KQueryUtils.getKRowNull(this.type);
    }
    
    public KCollection set(final String property, final KRowFunction kRowFunction) {
        for (final KRow kRow : list) {
            kRow.set(property, kRowFunction.run(kRow));
        }
        
        return this;
    }
    
    public KCollection set(final String property, final Object value) {
        for (final KRow kRow : list) {
            kRow.set(property, value);
        }
        
        return this;
    }
    
    public KCollection filter(final KRowFunction<KRow, Boolean> kRowFunction) {
        final KCollection kCollectionCloned = new KCollection(this.type, new ArrayList<>(), this.metadata);//new HashMap(this.ref), new HashMap(this.extra), table, new ArrayList<>(this.exclude)
        
        for (final KRow kRow : this.list) {
            if (kRowFunction.run(kRow)) {
                kCollectionCloned.list.add(kRow.cloneMe());
            }
        }
        
        return kCollectionCloned;
    }
    
    public KCollection cloneMe() {
        return new KCollection(this.type, new ArrayList<>(this.list), this.metadata);
    }
    
    public KCollection addProperty(final String property, final KRowFunction<KRow, Object> kRowFunction) {
        for (final KRow kRow : list) {
            kRow.addProperty(property, kRowFunction.run(kRow));
        }
        
//        this.ref.put(property, this.ref.entrySet().size());
        
        return this;
    }
    
    public KCollection addProperty(final String property, final Object value) {
        for (final KRow kRow : list) {
            kRow.addProperty(property, value);
        }
        
//        this.ref.put(property, this.ref.entrySet().size());
        
        return this;
    }
    
    public Iterator<T> iterator() {
        return list.iterator();
    }
    
    public <T, V> Map<T, V> twoColumnsToMap(final String key, final String value, final Class<T> clazzT, final Class<V> clazzV) {
        final Map<T, V> map = new HashMap<>();
        
        for (final KRow kRow : list) {
            map.put(kRow.getByClass(key, clazzT), kRow.getByClass(value, clazzV));
        }
        
        return map;
    }
    
    public <T> Map<T, KCollection> groupKCollectionByProperty(final String property, final boolean removeProperty, final Class<T> clazzT) {
        final Map<T, KCollection> map = new HashMap<>();
        
        for (final KRow kRow : this.list) {
            if (kRow.isNull(property)) {
                continue;
            }
            
            final T key = kRow.getByClass(property, clazzT);
            
            if (map.containsKey(key)) {
                final KCollection kCollection = map.get(key);
                
                kCollection.list.add((removeProperty) ? kRow.cloneMe().removeProperty(property) : kRow.cloneMe());

                map.put(key, kCollection);
            } else {
                map.put(key, new KCollection(this.type, new ArrayList() {{
                    add((removeProperty) ? kRow.cloneMe().removeProperty(property) : kRow.cloneMe());
                }}, this.metadata));
            }
        }
        
        return map;
    }
    
    public Map<Object, KCollection> groupKCollectionByObjectProperty(final String property) {
        return this.groupKCollectionByObjectProperty(property, false);
    }
    
    public Map<Object, KCollection> groupKCollectionByObjectProperty(final String property, final boolean removeProperty) {
        return this.groupKCollectionByProperty(property, removeProperty, Object.class);
    }
    
    public Map<String, KCollection> groupKCollectionByStringProperty(final String property) {
        return this.groupKCollectionByStringProperty(property, false);
    }
    
    public Map<String, KCollection> groupKCollectionByStringProperty(final String property, final boolean removeProperty) {
        return this.groupKCollectionByProperty(property, removeProperty, String.class);
    }
    
    public Map<Character, KCollection> groupKCollectionByCharacterProperty(final String property) {
        return this.groupKCollectionByCharacterProperty(property, false);
    }
    
    public Map<Character, KCollection> groupKCollectionByCharacterProperty(final String property, final boolean removeProperty) {
        return this.groupKCollectionByProperty(property, removeProperty, Character.class);
    }
    
    public Map<UUID, KCollection> groupKCollectionByUUIDProperty(final String property) {
        return this.groupKCollectionByUUIDProperty(property, false);
    }
    
    public Map<UUID, KCollection> groupKCollectionByUUIDProperty(final String property, final boolean removeProperty) {
        return this.groupKCollectionByProperty(property, removeProperty, UUID.class);
    }
    
    public Map<BigDecimal, KCollection> groupKCollectionByBigDecimalProperty(final String property) {
        return this.groupKCollectionByBigDecimalProperty(property, false);
    }
    
    public Map<BigDecimal, KCollection> groupKCollectionByBigDecimalProperty(final String property, final boolean removeProperty) {
        return this.groupKCollectionByProperty(property, removeProperty, BigDecimal.class);
    }
    
    public Map<BigInteger, KCollection> groupKCollectionByBigIntegerProperty(final String property) {
        return this.groupKCollectionByBigIntegerProperty(property, false);
    }
    
    public Map<BigInteger, KCollection> groupKCollectionByBigIntegerProperty(final String property, final boolean removeProperty) {
        return this.groupKCollectionByProperty(property, removeProperty, BigInteger.class);
    }
    
    public Map<Long, KCollection> groupKCollectionByLongProperty(final String property) {
        return this.groupKCollectionByLongProperty(property, false);
    }
    
    public Map<Long, KCollection> groupKCollectionByLongProperty(final String property, final boolean removeProperty) {
        return this.groupKCollectionByProperty(property, removeProperty, Long.class);
    }
    
    public Map<Integer, KCollection> groupKCollectionByIntegerProperty(final String property) {
        return this.groupKCollectionByIntegerProperty(property, false);
    }
    
    public Map<Integer, KCollection> groupKCollectionByIntegerProperty(final String property, final boolean removeProperty) {
        return this.groupKCollectionByProperty(property, removeProperty, Integer.class);
    }
    
    public Map<Boolean, KCollection> groupKCollectionByBooleanProperty(final String property) {
        return this.groupKCollectionByBooleanProperty(property, false);
    }
    
    public Map<Boolean, KCollection> groupKCollectionByBooleanProperty(final String property, final boolean removeProperty) {
        return this.groupKCollectionByProperty(property, removeProperty, Boolean.class);
    }
    
    public Map<Double, KCollection> groupKCollectionByDoubleProperty(final String property) {
        return this.groupKCollectionByDoubleProperty(property, false);
    }
    
    public Map<Double, KCollection> groupKCollectionByDoubleProperty(final String property, final boolean removeProperty) {
        return this.groupKCollectionByProperty(property, removeProperty, Double.class);
    }
    
    public Map<LocalDateTime, KCollection> groupKCollectionByLocalDateTimeProperty(final String property) {
        return this.groupKCollectionByLocalDateTimeProperty(property, false);
    }
    
    public Map<LocalDateTime, KCollection> groupKCollectionByLocalDateTimeProperty(final String property, final boolean removeProperty) {
        return this.groupKCollectionByProperty(property, removeProperty, LocalDateTime.class);
    }
    
    public Map<LocalDate, KCollection> groupKCollectionByLocalDateProperty(final String property) {
        return this.groupKCollectionByLocalDateProperty(property, false);
    }
    
    public Map<LocalDate, KCollection> groupKCollectionByLocalDateProperty(final String property, final boolean removeProperty) {
        return this.groupKCollectionByProperty(property, removeProperty, LocalDate.class);
    }
    
    public Map<Date, KCollection> groupKCollectionByDateProperty(final String property) {
        return this.groupKCollectionByDateProperty(property, false);
    }
    
    public Map<Date, KCollection> groupKCollectionByDateProperty(final String property, final boolean removeProperty) {
        return this.groupKCollectionByProperty(property, removeProperty, Date.class);
    }
    
    public Map<Timestamp, KCollection> groupKCollectionByTimestampProperty(final String property) {
        return this.groupKCollectionByTimestampProperty(property, false);
    }
    
    public Map<Timestamp, KCollection> groupKCollectionByTimestampProperty(final String property, final boolean removeProperty) {
        return this.groupKCollectionByProperty(property, removeProperty, Timestamp.class);
    }
    
    private <T> Map<T, List<Map<String, Object>>> groupListByProperty(final String property, final boolean removeProperty, final Class<T> clazzT) {
        final Map<T, List<Map<String, Object>>> map = new HashMap<>();
        
        for (final KRow kRow : this.list) {
            if (kRow.isNull(property)) {
                continue;
            }
            
            final T key = kRow.getByClass(property, clazzT);
            
            if (map.containsKey(key)) {
                final List<Map<String, Object>> list_ = map.get(key);
                
                list_.add((removeProperty) ? kRow.cloneMe().removeProperty(property).toMap() : kRow.cloneMe().toMap());
                
                map.put(key, list_);
            } else {
                map.put(key, new ArrayList<Map<String, Object>>() {{
                    add((removeProperty) ? kRow.cloneMe().removeProperty(property).toMap() : kRow.cloneMe().toMap());
                }});
            }
        }
        
        return map;
    }
    
    public Map<Object, List<Map<String, Object>>> groupListByObjectProperty(final String property) {
        return this.groupListByObjectProperty(property, false);
    }
    
    public Map<Object, List<Map<String, Object>>> groupListByObjectProperty(final String property, final boolean removeProperty) {
        return this.groupListByProperty(property, removeProperty, Object.class);
    }
    
    public Map<String, List<Map<String, Object>>> groupListByStringProperty(final String property) {
        return this.groupListByStringProperty(property, false);
    }
    
    public Map<String, List<Map<String, Object>>> groupListByStringProperty(final String property, final boolean removeProperty) {
        return this.groupListByProperty(property, removeProperty, String.class);
    }
    
    public Map<Character, List<Map<String, Object>>> groupListByCharacterProperty(final String property) {
        return this.groupListByCharacterProperty(property, false);
    }
    
    public Map<Character, List<Map<String, Object>>> groupListByCharacterProperty(final String property, final boolean removeProperty) {
        return this.groupListByProperty(property, removeProperty, Character.class);
    }
    
    public Map<UUID, List<Map<String, Object>>> groupListByUUIDProperty(final String property) {
        return this.groupListByUUIDProperty(property, false);
    }
    
    public Map<UUID, List<Map<String, Object>>> groupListByUUIDProperty(final String property, final boolean removeProperty) {
        return this.groupListByProperty(property, removeProperty, UUID.class);
    }
    
    public Map<BigDecimal, List<Map<String, Object>>> groupListByBigDecimalProperty(final String property) {
        return this.groupListByBigDecimalProperty(property, false);
    }
    
    public Map<BigDecimal, List<Map<String, Object>>> groupListByBigDecimalProperty(final String property, final boolean removeProperty) {
        return this.groupListByProperty(property, removeProperty, BigDecimal.class);
    }
    
    public Map<BigInteger, List<Map<String, Object>>> groupListByBigIntegerProperty(final String property) {
        return this.groupListByBigIntegerProperty(property, false);
    }
    
    public Map<BigInteger, List<Map<String, Object>>> groupListByBigIntegerProperty(final String property, final boolean removeProperty) {
        return this.groupListByProperty(property, removeProperty, BigInteger.class);
    }
    
    public Map<Long, List<Map<String, Object>>> groupListByLongProperty(final String property) {
        return this.groupListByLongProperty(property, false);
    }
    
    public Map<Long, List<Map<String, Object>>> groupListByLongProperty(final String property, final boolean removeProperty) {
        return this.groupListByProperty(property, removeProperty, Long.class);
    }
    
    public Map<Integer, List<Map<String, Object>>> groupListByIntegerProperty(final String property) {
        return this.groupListByIntegerProperty(property, false);
    }
    
    public Map<Integer, List<Map<String, Object>>> groupListByIntegerProperty(final String property, final boolean removeProperty) {
        return this.groupListByProperty(property, removeProperty, Integer.class);
    }
    
    public Map<Boolean, List<Map<String, Object>>> groupListByBooleanProperty(final String property) {
        return this.groupListByBooleanProperty(property, false);
    }
    
    public Map<Boolean, List<Map<String, Object>>> groupListByBooleanProperty(final String property, final boolean removeProperty) {
        return this.groupListByProperty(property, removeProperty, Boolean.class);
    }
    
    public Map<Double, List<Map<String, Object>>> groupListByDoubleProperty(final String property) {
        return this.groupListByDoubleProperty(property, false);
    }
    
    public Map<Double, List<Map<String, Object>>> groupListByDoubleProperty(final String property, final boolean removeProperty) {
        return this.groupListByProperty(property, removeProperty, Double.class);
    }
    
    public Map<LocalDateTime, List<Map<String, Object>>> groupListByLocalDateTimeProperty(final String property) {
        return this.groupListByLocalDateTimeProperty(property, false);
    }
    
    public Map<LocalDateTime, List<Map<String, Object>>> groupListByLocalDateTimeProperty(final String property, final boolean removeProperty) {
        return this.groupListByProperty(property, removeProperty, LocalDateTime.class);
    }
    
    public Map<LocalDate, List<Map<String, Object>>> groupListByLocalDateProperty(final String property) {
        return this.groupListByLocalDateProperty(property, false);
    }
    
    public Map<LocalDate, List<Map<String, Object>>> groupListByLocalDateProperty(final String property, final boolean removeProperty) {
        return this.groupListByProperty(property, removeProperty, LocalDate.class);
    }
    
    public Map<Date, List<Map<String, Object>>> groupListByDateProperty(final String property) {
        return this.groupListByDateProperty(property, false);
    }
    
    public Map<Date, List<Map<String, Object>>> groupListByDateProperty(final String property, final boolean removeProperty) {
        return this.groupListByProperty(property, removeProperty, Date.class);
    }
    
    public Map<Timestamp, List<Map<String, Object>>> groupListByTimestampProperty(final String property) {
        return this.groupListByTimestampProperty(property, false);
    }
    
    public Map<Timestamp, List<Map<String, Object>>> groupListByTimestampProperty(final String property, final boolean removeProperty) {
        return this.groupListByProperty(property, removeProperty, Timestamp.class);
    }
    
    public KCollection addMetadata(final String key, final Object value) {
        this.metadata.put(key, value);
        
        return this;
    }
    
    public Map<String, Object> getMetadata() {
        return this.metadata;
    }
    
    public Object getMetadata(final String key) {
        return this.metadata.get(key);
    }
    
    public void clearMetadata() {
        this.metadata.clear();
    }
    
    public List<T> getList() {
        return this.list;
    }
}
