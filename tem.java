package test;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Date;

public class tem {
    public class JoinNC_Order extends CoProcessFunction<Tuple8<Integer, String, String, Integer, String, Double, String, String>,
            Tuple3<Integer,Integer, Date>,
            Tuple9<Integer, String, String, Integer, String, Double, String,String,Integer>> {
        private ListState<Tuple8<Integer, String, String, Integer, String, Double, String, String>> prevState;
        private ListState<Tuple3<Integer,Integer,Date>> orderState;

        //有运行上下文的时候
        @Override
        public void open(Configuration parameters) throws Exception {
            prevState  = getRuntimeContext().getListState(
                    new ListStateDescriptor<Tuple8<Integer, String, String, Integer, String, Double, String, String>>("nation_customer",
                            Types.TUPLE(Types.INT, Types.STRING,Types.STRING,Types.INT,Types.STRING, Types.DOUBLE, Types.STRING,Types.STRING))
            );
            orderState = getRuntimeContext().getListState(
                    new ListStateDescriptor<Tuple3<Integer,Integer,Date>>("order",
                            Types.TUPLE(Types.INT,Types.INT, Types.SQL_DATE))
            );
        }

        @Override
        public void processElement1(Tuple8<Integer, String, String, Integer, String, Double, String, String> value, CoProcessFunction<Tuple8<Integer, String, String,
                Integer, String, Double, String, String>, Tuple3<Integer, Integer, Date>, Tuple9<Integer, String, String, Integer, String, Double,
                String, String, Integer>>.Context context, Collector<Tuple9<Integer, String, String, Integer, String, Double, String, String, Integer>> out) throws Exception {
            //来的是prev tuple,进入这个实例的是具有相同nation key的tuple
            prevState.add(value);
            for(Tuple3<Integer,Integer,Date> elem : orderState.get()){
                //if matched
                //out.collect("thirdParty state contains"+elem);
                Tuple9<Integer, String, String, Integer, String, Double, String,String,Integer> res = new Tuple9<>(elem.f0,elem.f1,
                        elem.f2,elem.f3, elem.f4,elem.f5,elem.f6, value.f1);
                out.collect(res);
                //else; store it in unmatched
            }
        }

        @Override
        public void processElement2(Tuple3<Integer, Integer, Date> integerIntegerDateTuple3, CoProcessFunction<Tuple8<Integer, String, String, Integer, String, Double, String, String>, Tuple3<Integer, Integer, Date>, Tuple9<Integer, String, String, Integer, String, Double, String, String, Integer>>.Context context, Collector<Tuple9<Integer, String, String, Integer, String, Double, String, String, Integer>> collector) throws Exception {

        }
    }

}

