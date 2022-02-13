package com.alibaba.c2m.module03.topn;

/**
 * @author: spoon
 * @email: shaozejing@xiaomi.com
 * @create: 2022/2/12-4:32 PM
 */

public class OrderDetail {

    private String id;

    private Double amt;

    private Long time;

    public OrderDetail() {
    }

    public OrderDetail(String id, Double amt, Long time) {
        this.id = id;
        this.amt = amt;
        this.time = time;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Double getAmt() {
        return amt;
    }

    public void setAmt(Double amt) {
        this.amt = amt;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "OrderDetail{" +
            "id='" + id + '\'' +
            ", amt=" + amt +
            ", time=" + time +
            '}';
    }

}
