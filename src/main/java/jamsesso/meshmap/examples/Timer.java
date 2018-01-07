package jamsesso.meshmap.examples;

import java.util.LongSummaryStatistics;
import java.util.function.Consumer;
import java.util.stream.LongStream;

import static java.lang.System.out;

public class Timer {
  public static void time(String name, int iterations, Consumer<Integer> iteration) {
    long[] timings = new long[iterations];
    out.println("Started " + name + "...");

    for (int i = 0; i < iterations; i++) {
      long iterationStartTime = System.currentTimeMillis();
      iteration.accept(i);
      timings[i] = System.currentTimeMillis() - iterationStartTime;
    }

    LongSummaryStatistics stats = LongStream.of(timings).summaryStatistics();
    long elapsedTime = stats.getSum();
    long maxTime = stats.getMax();
    long minTime = stats.getMin();
    double avgTime = stats.getAverage();

    out.println(name + " took " + elapsedTime + "ms " +
                "(max=" + maxTime + "ms, min=" + minTime + ", avg=" + avgTime + ")");
  }
}
