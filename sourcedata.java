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

		DataStream<String> ds = env.readTextFile("/Users/judith/Project/StreamingData/data/nation.csv");

		// 通过map转换，将事件流中事件的数据类型变换为(word,1)元组形式
		DataStream<Tuple4<String,String,String,String>> ds_map =
				ds.map(new org.apache.flink.api.common.functions.MapFunction<String, Tuple4<String,String,String,String>>() {
					@Override
					public Tuple4<String,String,String,String> map(String s) throws Exception {
						String[] word = s.split("\\|");
						return new Tuple4<>(word[0],word[1],word[2],word[3]);
					}
				});
		ds_map.print();

		// 执行
		env.execute("flink keyBy transformatiion");
	}

	/*public static class Splitter implements FlatMapFunction<String, Tuple4<Integer, String,Integer,String>> {
		@Override
		public void flatMap(String sentence, Collector<Tuple4<Integer, String,Integer,String>> out) throws Exception {
			System.out.println(sentence);
			String[] word = sentence.split("|");
			out.collect(new Tuple4<Integer, String,Integer,String>(Integer.parseInt(word[0]), word[1],Integer.parseInt(word[2]),word[3]));

		}
	}*/

}
