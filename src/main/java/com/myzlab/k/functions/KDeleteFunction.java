package com.myzlab.k.functions;

import com.myzlab.k.KWhereDelete;

@FunctionalInterface
public interface KDeleteFunction<KDeleteFrom, T extends KWhereDelete> {
    T run(KDeleteFrom kDeleteFrom);
}
