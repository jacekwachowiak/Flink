package master2017.flink;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Objects;

public class VehicleTelematics {
    public static void main(String[] args) throws Exception
    {
        System.out.println("The input file path and output folder path are the following:");
        System.out.println(args[0]);
        System.out.println(args[1]);

        final Short segmentMin = 52;
        final Short segmentMax = 56;
        Integer parall = 10; //4 for the machine

        if (args.length != 2)
        {
            throw new Exception("No proper number of arguments");
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();//.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream data = env.readTextFile(args[0]).setParallelism(1);
        String outputFile = args[1];

// exercise 1 - 90mph fine detector
        SingleOutputStreamOperator<Tuple6<String, String, String, String, String, Short>> outputEx1 = data.flatMap(new FlatMapFunction<String, Tuple6<String, String, String, String, String, Short>>() {

                                                                                                                     @Override
                                                                                                                     public void flatMap(String input, Collector<Tuple6<String, String, String, String, String, Short>> collector) throws Exception {
                                                                                                                         String[] wordArray = input.split(",");
                                                                                                                         if (wordArray.length != 8) {
                                                                                                                             throw new Exception("Format not proper");
                                                                                                                         }
                                                                                                                         Short speed = Short.parseShort(wordArray[2]);
                                                                                                                         if (speed > 90)
                                                                                                                         {
                                                                                                                         Tuple6<String, String, String, String, String, Short> outputRow = new Tuple6<String, String, String, String, String, Short>(wordArray[0], wordArray[1], wordArray[3], wordArray[6], wordArray[5], speed);
                                                                                                                         collector.collect(outputRow);
                                                                                                                         }
                                                                                                                     }
                                                                                                                 });
//end of ex 1

//data preparation for ex2 and ex3
        SingleOutputStreamOperator<Tuple8<Short, Integer, Short, Short, Short, Short, Short, Integer>> partitionedData  = data.map(new MapFunction<String, Tuple8<Short, Integer, Short, Short, Short, Short, Short, Integer>>() {

            @Override
         public Tuple8<Short, Integer, Short, Short, Short, Short, Short, Integer> map(String value) throws Exception {
                String[] wordArray = value.split(",");
                if (wordArray.length != 8) {
                    throw new Exception("Format not proper");
               }
                return new Tuple8<>(Short.parseShort(wordArray[0]), Integer.parseInt(wordArray[1]), Short.parseShort(wordArray[2]), Short.parseShort(wordArray[3]), Short.parseShort(wordArray[4]), Short.parseShort(wordArray[5]), Short.parseShort(wordArray[6]), Integer.parseInt(wordArray[7]) );
            }
        });

// exercise 2 - section 52-56 avg speed detector
        SingleOutputStreamOperator<Tuple8<Short, Integer, Short, Short, Short, Short, Short, Integer>> valuesInSpecificSegments = partitionedData.filter(new FilterFunction<Tuple8<Short, Integer, Short, Short, Short, Short, Short, Integer>>() {
            @Override
            public boolean filter(Tuple8<Short, Integer, Short, Short, Short, Short, Short, Integer> value) throws Exception {
                return value.f6 >= segmentMin && value.f6 <=segmentMax;
            }
        });//.setParallelism(1);

        SingleOutputStreamOperator<Tuple9<Integer, Short, Short, Integer, Integer, Short, Short, Short, Short>> partitionedPreparedData  = valuesInSpecificSegments.map(new MapFunction<Tuple8<Short, Integer, Short, Short, Short, Short, Short, Integer>, Tuple9<Integer, Short, Short, Integer, Integer, Short, Short, Short, Short>>() {

            @Override
            public Tuple9<Integer, Short, Short, Integer, Integer, Short, Short, Short, Short> map(Tuple8<Short, Integer, Short, Short, Short, Short, Short, Integer> value) throws Exception {
                return new Tuple9<> (value.f1, value.f0, value.f0, value.f7, value.f7, value.f6, value.f6, value.f3, value.f5);
            }
        });

        KeyedStream<Tuple9<Integer, Short, Short, Integer, Integer, Short, Short, Short, Short>, Tuple> timeStampedData =  partitionedPreparedData.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple9<Integer, Short, Short, Integer, Integer, Short, Short, Short, Short>>() {
            @Override
            public long extractAscendingTimestamp(Tuple9<Integer, Short, Short, Integer, Integer, Short, Short, Short, Short> element) {
                return element.f1 * 1000;
            }
        }).keyBy(0);

        SingleOutputStreamOperator<Tuple9<Integer, Short, Short, Integer, Integer, Short, Short, Short, Short>> singlePasses = timeStampedData
    .window(EventTimeSessionWindows.withGap(Time.seconds(31)))
                .reduce(new ReduceFunction<Tuple9<Integer, Short, Short, Integer, Integer, Short, Short, Short, Short>>() {
                    @Override
                    public Tuple9<Integer, Short, Short, Integer, Integer, Short, Short, Short, Short> reduce(Tuple9<Integer, Short, Short, Integer, Integer, Short, Short, Short, Short> value1, Tuple9<Integer, Short, Short, Integer, Integer, Short, Short, Short, Short> value2) throws Exception {

                        if (value1.f1 > value2.f1)
                            value1.f1 = value2.f1;
                        if (value1.f2 < value2.f2)
                            value1.f2 = value2.f2;
                        if (value1.f3 > value2.f3)
                            value1.f3 = value2.f3;
                        if (value1.f4 < value2.f4)
                            value1.f4 = value2.f4;
                        if (value1.f5 > value2.f5)
                            value1.f5 = value2.f5;
                        if (value1.f6 < value2.f6)
                            value1.f6 = value2.f6;
                        return value1;

                    }
                }).setParallelism(1);

        SingleOutputStreamOperator<Tuple9<Integer, Short, Short, Integer, Integer, Short, Short, Short, Short>> legalSinglePasses = singlePasses.filter(new FilterFunction<Tuple9<Integer, Short, Short, Integer, Integer, Short, Short, Short, Short>>() {
            @Override
            public boolean filter(Tuple9<Integer, Short, Short, Integer, Integer, Short, Short, Short, Short> value) throws Exception {
                return Objects.equals(value.f5, segmentMin) && Objects.equals(value.f6, segmentMax);
            }
        });

        SingleOutputStreamOperator<Tuple6<Short, Short, Integer, Short, Short, Short>> passesWithAverageSpeed = legalSinglePasses.map(new MapFunction<Tuple9<Integer, Short, Short, Integer, Integer, Short, Short, Short, Short>, Tuple6<Short, Short, Integer, Short, Short, Short>>() {
            @Override
            public Tuple6<Short, Short, Integer, Short, Short, Short> map(Tuple9<Integer, Short, Short, Integer, Integer, Short, Short, Short, Short> value) throws Exception {
                Short averageSpeed = 0;
                if(!Objects.equals(value.f1, value.f2))
                {
                    averageSpeed = (short) (((double)(value.f4 - value.f3) / (double)(value.f2 - value.f1)) * 2.23694); //conversion from feet/second to mile/hour
                }
                return new Tuple6<>(value.f1, value.f2, value.f0, value.f7, value.f8, averageSpeed);

            }
        });

        SingleOutputStreamOperator<Tuple6<Short, Short, Integer, Short, Short, Short>> outputEx2 = passesWithAverageSpeed.filter(new FilterFunction<Tuple6<Short, Short, Integer, Short, Short, Short>>() {
            @Override
            public boolean filter(Tuple6<Short, Short, Integer, Short, Short, Short> value) throws Exception {
                return value.f5 >= 60; // short/integer cuts off the rest so 60.55->60
            }
        });

        // ex 3 accidents
        final SingleOutputStreamOperator<Tuple7<Short, Short, Integer, Short, Short, Short, Integer>> partitionedPreparedData2  = partitionedData.map(new MapFunction<Tuple8<Short, Integer, Short, Short, Short, Short, Short, Integer>, Tuple7<Short, Short, Integer, Short, Short, Short, Integer>>() {

            @Override
            public Tuple7<Short, Short, Integer, Short, Short, Short, Integer> map(Tuple8<Short, Integer, Short, Short, Short, Short, Short, Integer> value) throws Exception {
                return new Tuple7<> (value.f0, value.f0, value.f1, value.f3, value.f6, value.f5, value.f7);
            }
        });

        SingleOutputStreamOperator<Tuple7<Short, Short, Integer, Short, Short, Short, Integer>> outputEx3 = partitionedPreparedData2.setParallelism(1).keyBy(2)
                .countWindow(4, 1).apply(new WindowFunction<Tuple7<Short, Short, Integer, Short, Short, Short, Integer>, Tuple7<Short, Short, Integer, Short, Short, Short, Integer>, Tuple, GlobalWindow>() {

                    @Override
                    public void apply(Tuple tuple, GlobalWindow window, Iterable<Tuple7<Short, Short, Integer, Short, Short, Short, Integer>> input, Collector<Tuple7<Short, Short, Integer, Short, Short, Short, Integer>> out) throws Exception {

                        Iterator<Tuple7<Short, Short, Integer, Short, Short, Short, Integer>> iter = input.iterator();
                        Tuple7<Short, Short, Integer, Short, Short, Short, Integer> tempTuple = iter.next();
                        Integer pos = tempTuple.f6;
                        Integer count = 1;
                        while (iter.hasNext())
                        {
                            tempTuple = iter.next();
                            if (Objects.equals(pos, tempTuple.f6))
                            {
                                count++;
                            }
                            else
                            {
                                break;
                            }

                        }
                        if (count == 4)
                        {

                            tempTuple.f1 = (short)(tempTuple.f0 +90);

                            out.collect(tempTuple);
                        }
                    }
                }).setParallelism(parall);

        outputEx1.writeAsCsv(outputFile + "speedfines.csv");
        outputEx2.writeAsCsv(outputFile + "avgspeedfines.csv");
        outputEx3.writeAsCsv(outputFile + "accidents.csv");
        env.execute();

    }
}
