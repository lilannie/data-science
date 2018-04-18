import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.StringTokenizer;

import org.apache.spark.api.java.function.PairFunction;

public class GraphCycles {
    private final static int numOfReducers = 150;

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: WordCount <output>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("Lab 8 Exp 1");
        JavaSparkContext context = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = context.textFile("/cpre419/patents.txt");

        // Combine both edge sets
        JavaPairRDD<String, String> all_edges =

                // Map each edge ==> <vertex_1, vertex_2>
                lines.mapToPair(new PairFunction<String, String, String>() {
                    @Override
                    public Tuple2<String, String> call(String s) {
                        StringTokenizer tokens = new StringTokenizer(s);
                        return new Tuple2<String, String>(tokens.nextToken(), tokens.nextToken());
                    }
                })

                // Map each edge ==> <vertex_2, vertex_1>
                .union(lines.mapToPair(new PairFunction<String, String, String>() {
                    @Override
                    public Tuple2<String, String> call(String s) {
                        StringTokenizer tokens = new StringTokenizer(s);
                        String vertex1 = tokens.nextToken();
                        return new Tuple2<String, String>(tokens.nextToken(), vertex1);
                    }
                }));

        all_edges = all_edges.repartition(numOfReducers);

        // Group outgoing edges to get an adjacency list
        JavaPairRDD<String, Iterable<String>> adjacencyLists = all_edges.groupByKey(numOfReducers);

        adjacencyLists = adjacencyLists.repartition(numOfReducers);

        // Group each edge by the nodes it can reach in either one hop or two hops
        JavaPairRDD<Tuple2<String, String>, Iterable<String>> neighbors = adjacencyLists
                .flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<String>>,
                        Tuple2<String, String>, Iterable<String>>() {

                    @Override
                    public Iterable<Tuple2<Tuple2<String, String>, Iterable<String>>>
                        call(Tuple2<String , Iterable<String>> adjacencyList) throws Exception {

                            LinkedList<Tuple2<Tuple2<String, String>,
                                    Iterable<String>>> neighborhoods = new LinkedList<>();

                            for (String neighbor: adjacencyList._2) {
                                neighborhoods.add(new Tuple2<Tuple2<String, String>, Iterable<String>>(
                                        new Tuple2<>(adjacencyList._1, neighbor),
                                        adjacencyList._2
                                ));
                                neighborhoods.add(new Tuple2<Tuple2<String, String>, Iterable<String>>(
                                        new Tuple2<>(neighbor, adjacencyList._1),
                                        adjacencyList._2
                                ));
                            }
                            return neighborhoods;
                    }
                })
                .repartition(numOfReducers);

        // Count the number of triangles by taking the intersection of 2 hop neighborhoods
        JavaPairRDD<Integer, Integer> triangles =  neighbors
                .groupByKey(numOfReducers)
                .mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Iterable<Iterable<String>>>,
                        Integer, Integer>() {
                    @Override
                    public Tuple2<Integer, Integer>
                        call(Tuple2<Tuple2<String, String>, Iterable<Iterable<String>>> edge) {
                            HashSet<String> intersection = new HashSet<>();
                            Iterator<Iterable<String>> i = edge._2.iterator();

                            Iterable<String> firstList = i.next();
                            if (!i.hasNext())  new Tuple2<>(edge._1, new HashSet<>());

                            Iterable<String> secondList = i.next();
                            HashSet<String> secondListHash = new HashSet<>();
                            for (String neighbor: secondList) {
                                secondListHash.add(neighbor);
                            }

                            for (String neighbor: firstList) {
                                if (secondListHash.contains(neighbor)) intersection.add(neighbor);
                            }

                            return new Tuple2<>(1, intersection.size());
                    }
                });

        // Sum up the number of triangles
        JavaPairRDD<Integer, Integer> sumTriangles = triangles.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }, 1);

        // Output the number of cycles by dividing by 6 to remove overlapping cycles
        JavaPairRDD<String, Integer> numCycles = sumTriangles.mapToPair(new PairFunction<Tuple2<Integer,Integer>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer,Integer> t) throws Exception {
               return new Tuple2<String, Integer>("Number of Cycles", t._2/6);
            }
        });

        numCycles.saveAsTextFile(args[0]);
        context.stop();
        context.close();
    }
}
