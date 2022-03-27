package com.sachin.idgen.leaf.core.segment.dao;

import com.sachin.idgen.leaf.core.segment.model.LeafAlloc;
import org.apache.ibatis.annotations.*;

import java.util.List;

/**
 * @Author Sachin
 * @Date 2022/3/27
 **/
public interface IDAllocMapper {


    @Select("select big_tag ,max_id ,step, update_time from leaf_alloc")
    @Results(value = {
            @Result(column = "biz_tag", property = "key"),
            @Result(column = "max_id", property = "maxId"),
            @Result(column = "step", property = "step"),
            @Result(column = "update_time", property = "updateTime")
    })
    List<LeafAlloc> getAllLeafAllocs();

    @Select("select big_tag, max_id, step from leaf_alloc where big_tag= #{tag}")
    @Results(value = {
            @Result(column = "big_tag", property = "key"),
            @Result(column = "max_id", property = "maxId"),
            @Result(column = "step", property = "step")

    })
    LeafAlloc getLeafAlloc(@Param("tag") String tag);

    @Update("update leaf_alloc set max_id=max_id + step where big_tag= #{tag}")
    void updateMaxId(@Param("tag") String tag);

    @Update("update leaf_alloc set max_id =max_id + #{step} where big_tag= #{key}")
    void updateMaxIdByCustomStep(@Param("leafAlloc") LeafAlloc leafAlloc);

    @Select("select big_tag from leaf_alloc")
    List<String> getAllTags();



}
