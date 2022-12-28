package com.xueai8;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.util.Collector;

import org.apache.flink.core.fs.Path;
import org.apache.flink.util.OutputTag;
import org.apache.flink.api.java.utils.ParameterTool;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Nation {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		//获取数据源

		List data = new ArrayList<Tuple3<Integer,Integer,Integer>>();

		data.add(new Tuple3<>(0,1,0));

		data.add(new Tuple3<>(0,1,1));

		data.add(new Tuple3<>(0,2,2));

		data.add(new Tuple3<>(0,1,3));

		data.add(new Tuple3<>(1,2,5));

		data.add(new Tuple3<>(1,2,9));

		data.add(new Tuple3<>(1,2,11));

		data.add(new Tuple3<>(1,2,13));


		DataStreamSource<Tuple3<Integer,Integer,Integer>> items = env.fromCollection(data);

		OutputTag<Tuple3<Integer,Integer,Integer>> zeroStream = new OutputTag<Tuple3<Integer,Integer,Integer>>("zeroStream") {};

		OutputTag<Tuple3<Integer,Integer,Integer>> oneStream = new OutputTag<Tuple3<Integer,Integer,Integer>>("oneStream") {};


		SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> processStream= items.process(new ProcessFunction<Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {

			@Override
			public void processElement(Tuple3<Integer, Integer, Integer> value, Context ctx, Collector<Tuple3<Integer, Integer, Integer>> out) throws Exception {

				if (value.f0 == 0) {
					ctx.output(zeroStream, value);

				} else if (value.f0 == 1) {
					ctx.output(oneStream, value);

				}
			}

		});



		DataStream<Tuple3<Integer, Integer, Integer>> zeroSideOutput = processStream.getSideOutput(zeroStream);

		DataStream<Tuple3<Integer, Integer, Integer>> oneSideOutput = processStream.getSideOutput(oneStream);


		zeroSideOutput.print();

		oneSideOutput.printToErr();

		//打印结果
		String jobName = "user defined streaming source";

		env.execute(jobName);

	}

}

