package com.xueai8;
import apple.laf.JRSUIState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class Nation {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> nation = env.readTextFile("/Users/judith/Project/StreamingData/data/nation.csv");
		DataStream<String> customer = env.readTextFile("/Users/judith/Project/StreamingData/data/customer.csv");

		// 通过map转换，将事件流中事件的数据类型变换为(0,ALGERIA,0, haggle. carefully final deposits detect slyly agai)元组形式
		DataStream<Tuple2<Integer,String>> nation_map =
				nation.map(new org.apache.flink.api.common.functions.MapFunction<String, Tuple2<Integer,String>>() {
					@Override
					public Tuple2<Integer,String> map(String s) throws Exception {
						String[] word = s.split("\\|");
						return new Tuple2<>(Integer.valueOf(word[0]),word[1]);
					}
				});
		DataStream<Tuple7<Integer,String,String, Integer,String,Double, String>> customer_map =
				customer.map(new org.apache.flink.api.common.functions.MapFunction<String, Tuple7<Integer,String,String, Integer,String,Double,String>>() {
					@Override
					public Tuple7<Integer,String,String, Integer,String,Double,String> map(String s) throws Exception {
						String[] word = s.split("\\|");
						return new Tuple7<>(Integer.valueOf(word[0]),word[1],word[2],Integer.valueOf(word[3]),word[4],
								Double.valueOf(word[5]),word[7]);
					}
				});
		// keyBy转换，按key重分区
		KeyedStream<Tuple2<Integer, String>, Integer> nation_keyed = nation_map.keyBy(value -> value.f0);
		KeyedStream<Tuple7<Integer,String,String, Integer,String,Double,String>, Integer> customer_keyed = customer_map.keyBy(value -> value.f3);

		//connect two keyedstream
		nation_keyed.connect(customer_keyed)
				.process(new JoinMatchResult())
				.print();

		// 输出
		//nation_keyed.print();

		// 执行
		env.execute("Nation Job");
	}

	//自定义实现CoProcessFunction
	public static class JoinMatchResult extends CoProcessFunction<Tuple2<Integer,String>,
			Tuple7<Integer,String,String, Integer,String,Double,String>,
			String>{
		private ValueState<Tuple2<Integer, String>> nationState;
		private ValueState<Tuple7<Integer, String, String, Integer, String, Double, String>> customerState;
		//有运行上下文的时候

		@Override
		public void open(Configuration parameters) throws Exception {
			nationState  = getRuntimeContext().getState(
					new ValueStateDescriptor<Tuple2<Integer, String>>("nation", Types.TUPLE(Types.INT, Types.STRING))
			);
			customerState = getRuntimeContext().getState(
					new ValueStateDescriptor<Tuple7<Integer,String,String, Integer,String,Double,String>>("customer",
							Types.TUPLE(Types.INT,Types.STRING, Types.STRING, Types.INT,Types.STRING,Types.DOUBLE,Types.STRING))
			);
		}

		@Override
		public void processElement1(Tuple2<Integer, String> value,  Context ctx, Collector<String> out) throws Exception {
			//来的是nation tuple,进入这个实例的是具有相同nation key的tuple
			if (customerState.value() != null){
				out.collect("matched"+value+customerState.value());
				//清空状态
				//customerState.clear();
			} else{
				//更新状态
				nationState.update(value);
			}
		}

		@Override
		public void processElement2(Tuple7<Integer, String, String, Integer, String, Double, String> value, Context ctx, Collector<String> out) throws Exception {
			if (nationState.value() != null){
				out.collect("process element2 matched"+nationState.value()+" "+value);
				//清空状态
				//nationState.clear();
			} else{
				//更新状态
				customerState.update(value);
			}
		}

		//处理nation里面的tuple（相同的key做相同的处理）

	}

}
