package com.xueai8;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

public class Nation {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		System.out.println("new assignment");

		DataStream<Tuple3<String,String,Long>> app = env.fromElements(
				Tuple3.of("o1","app",300L),
				Tuple3.of("o2","app",400L),
				Tuple3.of("o3","app",500L)
		);
		DataStream<Tuple4<String,String,String,Long>> third_party = env.fromElements(
				Tuple4.of("o1","third_party","success",300L),
				Tuple4.of("o1","third_party","success",400L),
				Tuple4.of("o2","third_party","success",500L)
		);



		// keyBy转换，按key重分区
		KeyedStream<Tuple3<String,String,Long>, String> app_keyed = app.keyBy(value -> value.f0);
		KeyedStream<Tuple4<String,String,String,Long>, String> third_keyed = third_party.keyBy(value -> value.f0);

		//connect two keyedstream
		app_keyed.connect(third_keyed)
				.process(new NewMatchResult())
				.print();

		// 输出
		//nation_keyed.print();

		// 执行
		env.execute("Nation Job");
	}

	//自定义实现CoProcessFunction
	public static class NewMatchResult extends CoProcessFunction<Tuple3<String,String,Long>,
			Tuple4<String,String,String,Long>,
			String>{
		private ListState<Tuple3<String,String,Long>> appState;

		private ListState<Tuple4<String,String,String,Long>> thirdPartyState;
		//有运行上下文的时候

		@Override
		public void open(Configuration parameters) throws Exception {
			appState  = getRuntimeContext().getListState(
					new ListStateDescriptor<Tuple3<String,String,Long>>("my-app", Types.TUPLE(Types.STRING,Types.STRING,Types.LONG))
			);

			thirdPartyState = getRuntimeContext().getListState(
					new ListStateDescriptor<Tuple4<String,String,String,Long>>("third_party",
							Types.TUPLE(Types.STRING, Types.STRING,Types.STRING,Types.LONG))
			);
		}

		@Override
		public void processElement1(Tuple3<String, String, Long> value, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context context, Collector<String> out) throws Exception {
			//来的是APP tuple,进入这个实例的是具有相同nation key的tuple
			//new app tuple, if contain matched tuples, print out, if exist unmatched tuple, store it in unmatched state
			appState.add(value);
			for(Tuple4<String,String,String,Long> elem : thirdPartyState.get()){
				//if matched
				//out.collect("thirdParty state contains"+elem);
				out.collect("app matched third party"+value+elem);
				//else; store it in unmatched
			}
		}



		@Override
		public void processElement2(Tuple4<String, String, String, Long> value, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context context, Collector<String> out) throws Exception {
			thirdPartyState.add(value);

			for(Tuple3<String,String,Long> elem : appState.get()){
				//if matched
				//out.collect("appParty state contains"+elem);
				out.collect("2app matched third party"+elem+value);
				//else; store it in unmatched
			}


		}

		}

	}
