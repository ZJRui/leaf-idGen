package com.sachin.idgen.leaf.core.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author Sachin
 * @Date 2022/3/27
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Result {

    private long id;
    private Status status;

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Result{");
        sb.append("id=").append(id);
        sb.append(", status=").append(status);
        sb.append('}');
        return sb.toString();
    }
}
