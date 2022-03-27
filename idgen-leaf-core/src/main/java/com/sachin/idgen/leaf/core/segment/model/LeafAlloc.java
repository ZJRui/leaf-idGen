package com.sachin.idgen.leaf.core.segment.model;

import lombok.Data;

/**
 * @Author Sachin
 * @Date 2022/3/27
 **/
@Data
public class LeafAlloc {


    private String key;
    private long maxId;
    private int step;
    private String updateTime;
}
