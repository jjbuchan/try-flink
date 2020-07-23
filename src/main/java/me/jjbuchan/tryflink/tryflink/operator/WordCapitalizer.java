package me.jjbuchan.tryflink.tryflink.operator;

import org.apache.flink.api.common.functions.MapFunction;

public class WordCapitalizer implements MapFunction<String, String> {

  @Override
  public String map(String s) {
    return s.toUpperCase();
  }
}
