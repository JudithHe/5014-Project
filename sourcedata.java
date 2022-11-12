package com.xueai8;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamingJob {


  public static void main(String[] args) throws Exception {

      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

      DataStreamSource<String> dataStream = env.readTextFile("/Users/judith/Project/StreamingData/data/nation.csv");

      dataStream.print();

      env.execute("Window WordCount");
    }
}
