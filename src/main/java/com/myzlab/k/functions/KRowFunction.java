package com.myzlab.k.functions;

@FunctionalInterface
public interface KRowFunction<KRow, T> {
    T run(KRow kRow);
}
