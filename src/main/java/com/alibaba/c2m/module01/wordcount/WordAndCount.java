package com.alibaba.c2m.module01.wordcount;

/**
 * @author qingyong
 * @date 2022/02/08
 */
public class WordAndCount {

    private String word;

    private Integer frequency;

    public WordAndCount() {
    }

    public WordAndCount(String word, Integer frequency) {
        this.word = word;
        this.frequency = frequency;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getFrequency() {
        return frequency;
    }

    public void setFrequency(Integer frequency) {
        this.frequency = frequency;
    }

    @Override
    public String toString() {
        return "WordAndCount{" +
            "word='" + word + '\'' +
            ", count=" + frequency +
            '}';
    }

}
