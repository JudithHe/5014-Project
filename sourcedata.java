package com.xueai8;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class StreamingJob {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple4<Integer, String,Integer,String>> dataStream = env.readTextFile("/Users/judith/Project/StreamingData/data/nation.csv")
				.flatMap(new Splitter());

		dataStream.print();

		env.execute("Window WordCount");
	}

	public static class Splitter implements FlatMapFunction<String, Tuple4<Integer, String,Integer,String>> {
		@Override
		public void flatMap(String sentence, Collector<Tuple4<Integer, String,Integer,String>> out) throws Exception {
			System.out.println(sentence);
			String[] word = sentence.split("|");
			out.collect(new Tuple4<Integer, String,Integer,String>(Integer.parseInt(word[0]), word[1],Integer.parseInt(word[2]),word[3]));

		}
	}

}
