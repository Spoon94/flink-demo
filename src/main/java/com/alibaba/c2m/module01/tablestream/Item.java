package com.alibaba.c2m.module01.tablestream;

import java.io.Serializable;

/**
 * @author qingyong
 * @date 2022/02/08
 */
public class Item implements Serializable {

    private String name;

    private Integer id;

    public Item() {
    }

    public Item(String name, Integer id) {
        this.name = name;
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

}
