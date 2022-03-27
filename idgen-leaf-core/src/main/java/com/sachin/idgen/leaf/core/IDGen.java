package com.sachin.idgen.leaf.core;

import com.sachin.idgen.leaf.core.common.Result;

/**
 * @Author Sachin
 * @Date 2022/3/27
 **/
public interface IDGen {

    Result get(String key);

    boolean init();
}
