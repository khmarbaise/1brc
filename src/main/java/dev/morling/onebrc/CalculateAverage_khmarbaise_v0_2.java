/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.DoubleSummaryStatistics;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.groupingByConcurrent;

public class CalculateAverage_khmarbaise_v0_2 {

    private static final Path MEASUREMENT_FILES = Path.of("./measurements.txt");

    private record MeasurementRecord(String city, long measuredValue) {
    }

    static long parseIntoLong(String s, int start, int end) {
        long acc = 0;
        for (int i = start; i < end; i++) {
            if (acc != 0) {
                acc *= 10;
            }
            char c = s.charAt(i);
            var v = c - '0';
            acc += v;
        }
        return acc;
    }

    static long parseLong(String strValue) {
        int sign = +1;
        if (strValue.charAt(0) == '+' || strValue.charAt(0) == '-') {
            strValue = strValue.substring(1);
        }

        int positionPoint = strValue.indexOf('.');
        long preResult = parseIntoLong(strValue, 0, positionPoint);
        long postResult = parseIntoLong(strValue, positionPoint + 1, strValue.length());

        return (preResult * 10L + postResult) * sign;
    }

    private static final Function<String, MeasurementRecord> toMeasurementRecord = line -> {
        var posOf = line.indexOf(";");
        var city = line.substring(0, posOf);
        var measuredValue = line.substring(posOf + 1);
        return new MeasurementRecord(city, parseLong(measuredValue));
    };

    private static class OutputMetrics {
        private long min = Long.MAX_VALUE;
        private long max = Long.MIN_VALUE;
        private long sum;
        private long count;

        public OutputMetrics combine(OutputMetrics o) {
            var r = new OutputMetrics();
            r.min = Math.min(min, o.min);
            r.max = Math.max(max, o.max);
            r.sum = sum + o.sum;
            r.count = count + o.count;
            return r;
        }

        public void accumulate(MeasurementRecord m) {
            if (m.measuredValue < min)
                min = m.measuredValue();
            if (m.measuredValue > max)
                max = m.measuredValue();
            sum += m.measuredValue();
            count++;
        }

        @Override
        public String toString() {
            var mininum = this.min / 10.0;
            var mean = Math.round(this.sum / (double) count) / 10.0;
            var maximum = this.max / 10.0;
            return mininum + "/" + mean + "/" + maximum;
        }
    }

    public static void main(String[] args) throws IOException {

        try (var lines = Files.lines(MEASUREMENT_FILES)) {
            var resultList = lines
                    .parallel()
                    .map(toMeasurementRecord)
                    .collect(groupingByConcurrent(MeasurementRecord::city, Collector.of(
                            OutputMetrics::new,
                            OutputMetrics::accumulate,
                            OutputMetrics::combine,
                            OutputMetrics::toString)))
                    .entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(s -> s.getKey() + "=" + s.getValue()).collect(Collectors.joining(", "));
            System.out.println("{" + resultList + "}");
        }
    }
}
