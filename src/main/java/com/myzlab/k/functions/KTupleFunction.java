package com.myzlab.k.functions;

import java.util.List;

@FunctionalInterface
public interface KTupleFunction<T> {
    List<Object> run(T t);
}
