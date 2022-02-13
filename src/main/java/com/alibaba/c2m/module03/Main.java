package com.alibaba.c2m.module03;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import org.apache.commons.io.FileUtils;

/**
 * @author: spoon
 * @email: shaozejing@xiaomi.com
 * @create: 2022/2/12-4:31 PM
 */
public class Main {

    public static void main(String[] args) throws IOException {
        List<String> user = Lists.newArrayList("aa", "bb", "cc", "dd", "ee");
        List<String> out = Lists.newArrayList();
        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            out.add(user.get(random.nextInt(5))+","+random.nextInt(10)+","+random.nextInt(10));
        }
        for (int i = 0; i < 10; i++) {
            out.add(user.get(random.nextInt(5))+","+random.nextInt(10)+","+(random.nextInt(10)+10));
        }
        FileUtils.writeLines(new File("/Users/spoon/Code/Java/flink-demo/input/order"),out);
    }

}
