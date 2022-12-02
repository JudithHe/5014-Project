package test;

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

public class test {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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
        private ValueState<Tuple3<String,String,Long>> appState;
        private ValueState<Tuple4<String,String,String,Long>> thirdPartyState;
        //有运行上下文的时候

        @Override
        public void open(Configuration parameters) throws Exception {
            appState  = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple3<String,String,Long>>("app", Types.TUPLE(Types.STRING, Types.STRING,Types.LONG))
            );
            thirdPartyState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple4<String,String,String,Long>>("third_party",
                            Types.TUPLE(Types.STRING, Types.STRING,Types.STRING,Types.LONG))
            );
        }

        @Override
        public void processElement1(Tuple3<String, String, Long> value, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context context, Collector<String> out) throws Exception {
            //来的是APP tuple,进入这个实例的是具有相同nation key的tuple
            if (thirdPartyState.value() != null){
                out.collect("matched"+value+thirdPartyState.value());
                //清空状态
                thirdPartyState.clear();
            } else{
                //更新状态
                appState.update(value);
            }
        }

        @Override
        public void processElement2(Tuple4<String, String, String, Long> value, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context context, Collector<String> out) throws Exception {
            if (appState.value() != null){
                out.collect("process element2 matched"+appState.value()+" "+value);
                //清空状态
                appState.clear();
            } else{
                //更新状态
                thirdPartyState.update(value);
            }
        }


    }

}
