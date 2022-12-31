import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.util.Collector;

import org.apache.flink.core.fs.Path;
import java.text.SimpleDateFormat;
import java.util.Date;

public class StreamingData {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);
		//read input data
		DataStream<String> data25 = env.readTextFile("/Users/judith/Project/StreamingData/data/data25.csv");
		//DataStream<String> customer = env.readTextFile("/Users/judith/Project/StreamingData/data/data50.csv");
		//DataStream<String> orders = env.readTextFile("/Users/judith/Project/StreamingData/data/data75.csv");
		//DataStream<String> lineitem = env.readTextFile("/Users/judith/Project/StreamingData/data/data100.csv");

        //side stream
        final OutputTag<String> nationStream = new OutputTag<String>("nationStream"){};
        final OutputTag<String> customerStream = new OutputTag<String>("customerStream"){};
        final OutputTag<String> ordersStream = new OutputTag<String>("ordersStream"){};
        final OutputTag<String> lineitemStream = new OutputTag<String>("lineitemStream"){};

        SingleOutputStreamOperator<String> processStream= data25.process(new ProcessFunction<String, String>() {

			@Override
			public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                String valuenew = value;
                if (value.startsWith("\"")){
                    //remove double quotes in String
                    valuenew = value.substring(1, value.length() - 1);
                }
                String[] word = valuenew.split("\\|");
				if (word[word.length-1].equals("nation")) {
					ctx.output(nationStream, valuenew);
				} else if (word[word.length-1].equals("customer")) {
					ctx.output(customerStream, valuenew);
				} else if (word[word.length-1].equals("orders")) {
					ctx.output(ordersStream, valuenew);
				} else if (word[word.length-1].equals("lineitem")) {
					ctx.output(lineitemStream, valuenew);
				}
			}

		});

        DataStream<String> nation = processStream.getSideOutput(nationStream);
		DataStream<String> customer = processStream.getSideOutput(customerStream);
        DataStream<String> orders = processStream.getSideOutput(ordersStream);
		DataStream<String> lineitem = processStream.getSideOutput(lineitemStream);


		//nation.print();


		// 通过map转换，将事件流中事件的数据类型变换为(0,ALGERIA,0, haggle. carefully final deposits detect slyly agai)元组形式
		DataStream<Tuple2<Integer, String>> nation_map =
				nation.map(new org.apache.flink.api.common.functions.MapFunction<String, Tuple2<Integer, String>>() {
					@Override
					public Tuple2<Integer, String> map(String s) throws Exception {
						String[] word = s.split("\\|");
						return new Tuple2<>(Integer.valueOf(word[0]), word[1]);
					}
				}).filter(new FilterFunction<Tuple2<Integer, String>>() {
			@Override
			public boolean filter(Tuple2<Integer, String> value) throws Exception {
				return value.f1.equals("CANADA");
			}
		});
		//nation_map.writeAsCsv("/Users/judith/Project/StreamingData/data/nation_map.csv");
		DataStream<Tuple7<Integer, String, String, Integer, String, Double, String>> customer_map =
				customer.map(new org.apache.flink.api.common.functions.MapFunction<String, Tuple7<Integer, String, String, Integer, String, Double, String>>() {
					@Override
					public Tuple7<Integer, String, String, Integer, String, Double, String> map(String s) throws Exception {
						String[] word = s.split("\\|");
						return new Tuple7<>(Integer.valueOf(word[0]), word[1], word[2], Integer.valueOf(word[3]), word[4],
								Double.valueOf(word[5]), word[7]);
					}
				});
		//customer_map.print();
		//customer_map.writeAsCsv("/Users/judith/Project/StreamingData/data/customer_map.csv");
		SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd");
		Date DATE = sdformat.parse("1995-02-06");

		DataStream<Tuple3<Integer, Integer, Date>> order_map =
				orders.map(new org.apache.flink.api.common.functions.MapFunction<String, Tuple3<Integer, Integer, Date>>() {
					@Override
					public Tuple3<Integer, Integer, Date> map(String s) throws Exception {
						String[] word = s.split("\\|");
						Date orderdate = sdformat.parse(word[4]);
						return new Tuple3<>(Integer.valueOf(word[0]), Integer.valueOf(word[1]), orderdate);
					}
				}).filter(new FilterFunction<Tuple3<Integer, Integer, Date>>() {
					@Override
					public boolean filter(Tuple3<Integer, Integer, Date> value) throws Exception {
						return value.f2.compareTo(DATE) == 0;
					}
				});
		//order_map.writeAsCsv("/Users/judith/Project/StreamingData/data/order_map.csv");
		//filter order map
		DataStream<Tuple4<Integer, Double, Double, String>> line_map =
				lineitem.map(new org.apache.flink.api.common.functions.MapFunction<String, Tuple4<Integer, Double, Double, String>>() {
					@Override
					public Tuple4<Integer, Double, Double, String> map(String s) throws Exception {
						String[] word = s.split("\\|");
						return new Tuple4<>(Integer.valueOf(word[0]), Double.valueOf(word[5]), Double.valueOf(word[6]), word[8]);
					}
				}).filter(new FilterFunction<Tuple4<Integer, Double, Double, String>>() {
					@Override
					public boolean filter(Tuple4<Integer, Double, Double, String> value) throws Exception {
						return value.f3.equals("R");
					}
				});
		//line_map.writeAsCsv("/Users/judith/Project/StreamingData/data/line_map.csv");

		// keyBy转换，按key重分区
		KeyedStream<Tuple2<Integer, String>, Integer> nation_keyed = nation_map.keyBy(value -> value.f0);
		KeyedStream<Tuple7<Integer, String, String, Integer, String, Double, String>, Integer> customer_keyed = customer_map.keyBy(value -> value.f3);
		//connect two keyedstream
		SingleOutputStreamOperator<Tuple8<Integer, String, String, Integer, String, Double, String, String>> nation_customer
				= nation_keyed.connect(customer_keyed)
				.process(new JoinNationCust());

		//继续join orders： new keyby customerkey
		KeyedStream<Tuple8<Integer, String, String, Integer,
				String, Double, String, String>, Integer> nation_customer_keyed = nation_customer.keyBy(value -> value.f0);
		KeyedStream<Tuple3<Integer, Integer, Date>, Integer> orders_keyed = order_map.keyBy(value -> value.f1);
		SingleOutputStreamOperator<Tuple9<Integer, String, String, Integer, String, Double, String, String, Integer>> na_cust_order
				= nation_customer_keyed.connect(orders_keyed)
				.process(new JoinNC_Order());

		//继续join lineitem： new keyby customerkey
		KeyedStream<Tuple9<Integer, String, String, Integer,
				String, Double, String, String, Integer>, Integer> NCO_keyed = na_cust_order.keyBy(value -> value.f8);
		KeyedStream<Tuple4<Integer, Double, Double, String>, Integer> lineitem_keyed = line_map.keyBy(value -> value.f0);
		SingleOutputStreamOperator<Tuple11<Integer, String, String, Integer, String, Double, String, String, Integer, Double, Double>> whole_join
				= NCO_keyed.connect(lineitem_keyed)
				.process(new JoinNCO_Lineitem());


		// group by and show the result
		DataStream<Tuple12<String, Integer, String, String, Integer, String, Double, String, String, Integer, Double, Double>> whole_addkey =
				whole_join.map(new org.apache.flink.api.common.functions.MapFunction<Tuple11<Integer, String, String, Integer, String, Double, String, String, Integer, Double, Double>,
						Tuple12<String, Integer, String, String, Integer, String, Double, String, String, Integer, Double, Double>>() {
					@Override
					public Tuple12<String, Integer, String, String, Integer, String, Double, String, String, Integer, Double, Double> map(
							Tuple11<Integer, String, String, Integer, String, Double, String, String, Integer, Double, Double> value) throws Exception {
						String keystr = Integer.toString(value.f0)+value.f1+ Double.toString(value.f5)+value.f4+value.f7+value.f2+value.f6;
						return new Tuple12<>(keystr,value.f0, value.f1,value.f2,value.f3,value.f4,value.f5,value.f6,value.f7,value.f8,
								value.f9,value.f10);
					}
				});
		KeyedStream<Tuple12<String, Integer, String, String, Integer, String, Double, String, String, Integer, Double, Double>, String>
				whole_join_keyed = whole_addkey.keyBy(value -> value.f0);
		//use process function to do aggregation
		DataStream result = whole_join_keyed.process(new countRevenue());
		result.writeAsCsv("/Users/judith/Project/StreamingData/data/data25_res.csv");
		//result.writeAsCsv("/Users/judith/Project/StreamingData/data/data50_res.csv");
		//result.writeAsCsv("/Users/judith/Project/StreamingData/data/data75_res.csv");
		//result.writeAsCsv("/Users/judith/Project/StreamingData/data/data100_res.csv");
		result.print();
		//StreamingFileSink<String> sink = StreamingFileSink
				//.forRowFormat(new Path("/Users/judith/Project/StreamingData/data/output"),
						//new SimpleStringEncoder<String>("UTF-8"))
				//.build();

		//result.addSink(sink);
		// 执行
		env.execute("Nation Job");
	}

	/**
	 * The implementation of the ProcessFunction that maintains the count and timeouts
	 */
	public static class countRevenue
			extends KeyedProcessFunction<String, Tuple12<String, Integer, String, String, Integer, String, Double, String, String, Integer, Double, Double>,
			Tuple8<Integer,String,Double,Double,String,String,String,String>> {
		/** The state that is maintained by this process function */
		private  ValueState<Tuple8<Integer, String, Double, Double, String, String, String, String>> restupleState;

		@Override
		public void open(Configuration parameters) throws Exception {
			restupleState = getRuntimeContext().getState(new ValueStateDescriptor<>("restupleState", Types.TUPLE(Types.INT,
					Types.STRING,Types.DOUBLE,Types.DOUBLE,Types.STRING,Types.STRING,Types.STRING,Types.STRING)));
		}


		@Override
		public void processElement(Tuple12<String, Integer, String, String, Integer, String, Double, String, String, Integer, Double, Double> value,
								   Context ctx, Collector<Tuple8<Integer, String, Double, Double, String, String, String, String>> out) throws Exception {
			if(restupleState.value()==null){
				Double revenue = value.f10*(1-value.f11);
				Tuple8<Integer, String, Double, Double, String, String, String, String> output = new Tuple8<>(value.f1,value.f2,
						revenue,value.f6,value.f8,value.f3,value.f5,value.f7);
				restupleState.update(output);
				out.collect(output);
			}else{
				//tuple value exists, need to update revenue value;
				Double prev_revenue = restupleState.value().f2;
				Double new_revenue = prev_revenue + value.f10*(1-value.f11);
				Tuple8<Integer, String, Double, Double, String, String, String, String> newres = new Tuple8<>(value.f1,value.f2,
						new_revenue,value.f6,value.f8,value.f3,value.f5,value.f7);
				restupleState.update(newres);
				out.collect(newres);
			}

		}
	}


	public static class JoinNCO_Lineitem extends CoProcessFunction<Tuple9<Integer, String, String, Integer, String, Double, String, String, Integer>,
			Tuple4<Integer, Double, Double, String>,
			Tuple11<Integer, String, String, Integer, String, Double, String, String, Integer, Double, Double>>{
		private ListState<Tuple9<Integer, String, String, Integer, String, Double, String, String, Integer>> NCOState;
		private ListState<Tuple4<Integer, Double, Double, String>> lineitemState;

		//有运行上下文的时候
		@Override
		public void open(Configuration parameters) throws Exception {
			NCOState = getRuntimeContext().getListState(
					new ListStateDescriptor<Tuple9<Integer, String, String, Integer, String, Double, String, String, Integer>>(
							"nation_customer_orders",
							Types.TUPLE(Types.INT, Types.STRING, Types.STRING, Types.INT, Types.STRING, Types.DOUBLE, Types.STRING, Types.STRING, Types.INT))
			);
			lineitemState = getRuntimeContext().getListState(
					new ListStateDescriptor<Tuple4<Integer, Double, Double, String>>("lineitem",
							Types.TUPLE(Types.INT, Types.DOUBLE, Types.DOUBLE, Types.STRING))
			);
		}

		@Override
		public void processElement1(Tuple9<Integer, String, String, Integer, String, Double, String, String, Integer> value,
									Context ctx, Collector<Tuple11<Integer, String, String, Integer, String, Double, String, String, Integer, Double, Double>> out) throws Exception {
			//来的是nation_customer_order tuple,进入这个实例的是具有相同nation_customer_order key的tuple
			NCOState.add(value);
			for (Tuple4<Integer, Double, Double, String> elem : lineitemState.get()) {
				//if matched
				//out.collect("thirdParty state contains"+elem);
				Tuple11<Integer, String, String, Integer, String, Double, String, String, Integer, Double, Double> res =
						new Tuple11<>(value.f0, value.f1, value.f2, value.f3, value.f4, value.f5, value.f6, value.f7,
								value.f8, elem.f1,elem.f2);
				out.collect(res);
				//else; store it in unmatched
			}
		}

		@Override
		public void processElement2(Tuple4<Integer, Double, Double, String> value,
									Context ctx, Collector<Tuple11<Integer, String, String, Integer, String, Double, String, String, Integer, Double, Double>> out) throws Exception {
			//来的是lineitem tuple,进入这个实例的是具有相同nation_customer_order key的tuple
			lineitemState.add(value);
			for (Tuple9<Integer, String, String, Integer, String, Double, String, String, Integer> elem : NCOState.get()) {
				//if matched
				//out.collect("thirdParty state contains"+elem);
				Tuple11<Integer, String, String, Integer, String, Double, String, String, Integer, Double, Double> res =
						new Tuple11<>(elem.f0, elem.f1, elem.f2, elem.f3, elem.f4, elem.f5,elem.f6, elem.f7,
								elem.f8, value.f1,value.f2);
				out.collect(res);
				//else; store it in unmatched
			}
		}
	}

	public static class JoinNC_Order extends CoProcessFunction<Tuple8<Integer, String, String, Integer, String, Double, String, String>,
			Tuple3<Integer,Integer, Date>,
			Tuple9<Integer, String, String, Integer, String, Double, String,String,Integer>> {
		private ListState<Tuple8<Integer, String, String, Integer, String, Double, String, String>> NCState;
		private ListState<Tuple3<Integer, Integer, Date>> orderState;

		//有运行上下文的时候
		@Override
		public void open(Configuration parameters) throws Exception {
			NCState = getRuntimeContext().getListState(
					new ListStateDescriptor<Tuple8<Integer, String, String, Integer, String, Double, String, String>>("nation_customer",
							Types.TUPLE(Types.INT, Types.STRING, Types.STRING, Types.INT, Types.STRING, Types.DOUBLE, Types.STRING, Types.STRING))
			);
			orderState = getRuntimeContext().getListState(
					new ListStateDescriptor<Tuple3<Integer, Integer, Date>>("order",
							Types.TUPLE(Types.INT, Types.INT, Types.SQL_DATE))
			);
		}

		@Override
		public void processElement1(Tuple8<Integer, String, String, Integer, String, Double, String, String> value,
									Context ctx, Collector<Tuple9<Integer, String, String, Integer, String, Double, String, String, Integer>> out) throws Exception {
			//来的是nation_customer tuple,进入这个实例的是具有相同nation_customer key的tuple
			NCState.add(value);
			for (Tuple3<Integer, Integer, Date> elem : orderState.get()) {
				//if matched
				//out.collect("thirdParty state contains"+elem);
				Tuple9<Integer, String, String, Integer, String, Double, String, String, Integer> res = new Tuple9<>(value.f0, value.f1,
						value.f2, value.f3, value.f4, value.f5, value.f6, value.f7, elem.f0);
				out.collect(res);
				//else; store it in unmatched
			}
		}

		@Override
		public void processElement2(Tuple3<Integer, Integer, Date> value,
									Context ctx, Collector<Tuple9<Integer, String, String, Integer, String, Double, String, String, Integer>> out) throws Exception {
			//来的是order tuple,进入这个实例的是具有相同nation_customer key的tuple
			orderState.add(value);
			for (Tuple8<Integer, String, String, Integer, String, Double, String, String> elem : NCState.get()) {
				//if matched
				//out.collect("thirdParty state contains"+elem);
				Tuple9<Integer, String, String, Integer, String, Double, String, String, Integer> res = new Tuple9<>(elem.f0, elem.f1,
						elem.f2, elem.f3, elem.f4, elem.f5, elem.f6, elem.f7, value.f0);
				out.collect(res);
			}
		}
	}

	//自定义实现CoProcessFunction
	public static class JoinNationCust extends CoProcessFunction<Tuple2<Integer,String>,
			Tuple7<Integer,String,String, Integer,String,Double,String>,
			Tuple8<Integer, String, String, Integer, String, Double, String,String>>{
		private ListState<Tuple2<Integer, String>> nationState;
		private ListState<Tuple7<Integer, String, String, Integer, String, Double, String>> customerState;

		//有运行上下文的时候
		@Override
		public void open(Configuration parameters) throws Exception {
			nationState  = getRuntimeContext().getListState(
					new ListStateDescriptor<Tuple2<Integer, String>>("nation", Types.TUPLE(Types.INT, Types.STRING))
			);
			customerState = getRuntimeContext().getListState(
					new ListStateDescriptor<Tuple7<Integer,String,String, Integer,String,Double,String>>("customer",
							Types.TUPLE(Types.INT,Types.STRING, Types.STRING, Types.INT,Types.STRING,Types.DOUBLE,Types.STRING))
			);
		}

		@Override
		public void processElement1(Tuple2<Integer, String> value,  Context ctx, Collector<Tuple8<Integer, String, String, Integer, String, Double, String,String>> out) throws Exception {
			//来的是nation tuple,进入这个实例的是具有相同nation key的tuple
			nationState.add(value);
			for(Tuple7<Integer, String, String, Integer, String, Double, String> elem : customerState.get()){
				//if matched
				//out.collect("thirdParty state contains"+elem);
				Tuple8<Integer, String, String, Integer, String, Double, String,String> res = new Tuple8<>(elem.f0,elem.f1,
						elem.f2,elem.f3, elem.f4,elem.f5,elem.f6, value.f1);
				out.collect(res);

				//else; the new nation tuple doesn't match with the  current customer tuple
			}
		}

		@Override
		public void processElement2(Tuple7<Integer, String, String, Integer, String, Double, String> value, Context ctx, Collector<Tuple8<Integer, String, String, Integer, String, Double, String,String>> out) throws Exception {
			customerState.add(value);
			for(Tuple2<Integer, String> elem : nationState.get()){
				//if matched
				Tuple8<Integer, String, String, Integer, String, Double, String,String> res = new Tuple8<>(value.f0,value.f1,
						value.f2,value.f3, value.f4,value.f5, value.f6, elem.f1);
				out.collect(res);
				//else; store it in unmatched
			}
		}

		//处理nation里面的tuple（相同的key做相同的处理）

	}

}
