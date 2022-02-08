package com.alibaba.c2m.module01.tablestream;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author qingyong
 * @date 2022/02/08
 */
public class ItemFactory {

    private static final AtomicInteger ID_GEN = new AtomicInteger(1);

    public static Item generateItem() {
        int i = ID_GEN.getAndIncrement();
        ArrayList<String> list = new ArrayList();
        list.add("HAT");
        list.add("TIE");
        list.add("SHOE");
        Item item = new Item();
        item.setName(list.get(new Random().nextInt(3)));
        item.setId(i);
        return item;
    }

}
