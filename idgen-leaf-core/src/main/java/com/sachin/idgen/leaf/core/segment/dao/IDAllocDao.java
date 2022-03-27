package com.sachin.idgen.leaf.core.segment.dao;

import com.sachin.idgen.leaf.core.segment.model.LeafAlloc;

import java.util.List;

/**
 * @Author Sachin
 * @Date 2022/3/27
 **/
public interface IDAllocDao {

    List<LeafAlloc> getAllLeafAllocs();

    LeafAlloc updateMaxIdAndGetLeafAlloc(String tag);

    LeafAlloc updateMaxIdByCustomStepAndGetLeafAlloc(LeafAlloc leafAlloc);

    List<String> getAllTags();


}
