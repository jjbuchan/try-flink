package me.jjbuchan.tryflink.tryflink.operator;

import org.apache.flink.api.common.functions.AggregateFunction;

public class JoinString implements AggregateFunction<String, JoinString, String> {

  String joinedString = "";

  @Override
  public JoinString createAccumulator() {
    return new JoinString();
  }

  @Override
  public JoinString add(String s, JoinString joinString) {
    joinString.joinedString += s;
    return joinString;
  }

  @Override
  public String getResult(JoinString joinString) {
    return joinString.joinedString;
  }

  @Override
  public JoinString merge(JoinString joinString, JoinString acc1) {
    joinString.joinedString += acc1.joinedString;
    return joinString;
  }
}
