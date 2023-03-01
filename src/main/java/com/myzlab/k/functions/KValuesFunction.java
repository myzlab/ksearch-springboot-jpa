package com.myzlab.k.functions;

import java.util.List;

@FunctionalInterface
public interface KValuesFunction<T> {
    List<Object> run(T t);
}
