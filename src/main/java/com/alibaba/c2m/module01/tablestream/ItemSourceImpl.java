package com.alibaba.c2m.module01.tablestream;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author qingyong
 * @date 2022/02/08
 */
public class ItemSourceImpl implements SourceFunction<Item> {

    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Item> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(ItemFactory.generateItem());
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

}
