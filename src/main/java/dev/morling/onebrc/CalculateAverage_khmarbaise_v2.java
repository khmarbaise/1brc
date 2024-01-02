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
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributeView;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.DoubleSummaryStatistics;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

public class CalculateAverage_khmarbaise_v2 {

    private static final Path MEASUREMENT_FILES = Path.of("./measurements.txt");

    private record MeasurementRecord(String city, Double measuredValue) {
    }

  private record Positions(long startPosition, long endPosition) {


    @Override
    public String toString() {
      return "Positions{" +
             "startPosition=" + startPosition + " (" + HexFormat.of().toHexDigits(startPosition) + ")" +
             ", endPosition=" + endPosition + " (" + HexFormat.of().toHexDigits(endPosition) + ")" +
             '}';
    }
  }

    static Positions calculate(long startPosition, long endPosition) throws IOException {
        try (var measurementFileChannel = FileChannel.open(MEASUREMENT_FILES, StandardOpenOption.READ)) {

            var bufferPosition = endPosition - 128L;
            var map = measurementFileChannel.map(FileChannel.MapMode.READ_ONLY, bufferPosition, 128L);

            map.load(); // really necessary?
            var byteBuffer = map.asReadOnlyBuffer();

            int posOfCr = 127;
            while (posOfCr > 0) {
                var c = (char) byteBuffer.get(posOfCr);

                // On Unix like systems only '\n' while on Windows '\r''\n'.
                if (c == '\n') { // FIXME: Windows issue?
                    break;
                }
                posOfCr--;
            }
            return new Positions(startPosition, bufferPosition + posOfCr);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static List<MeasurementRecord> readOneChunk(Positions positions) {
        System.out.println(Thread.currentThread().getName());
        List<MeasurementRecord> readRecords = new ArrayList<>();
        byte[] byteBuffer = new byte[128];

        try (var measurementFileChannel = FileChannel.open(MEASUREMENT_FILES, StandardOpenOption.READ)) {
            var size = positions.endPosition() - positions.startPosition();
            var mappedByteBuffer = measurementFileChannel.map(FileChannel.MapMode.READ_ONLY, positions.startPosition(),
                    size + 1);

            mappedByteBuffer.load(); // really necessary?
            var readOnlyBuffer = mappedByteBuffer.asReadOnlyBuffer();

            var bufferPosition = 0;
            while (bufferPosition < size) {

                var bufferStart = bufferPosition;
                while (readOnlyBuffer.get(bufferPosition) != '\n' && readOnlyBuffer.get(bufferPosition) != '\r'
                        && (bufferPosition < size + 1)) {
                    bufferPosition++;
                }

                Arrays.fill(byteBuffer, (byte) 0);
                readOnlyBuffer.get(byteBuffer, bufferStart, bufferPosition - bufferStart);
                // for (int i = bufferStart; i < bufferPosition; i++) {
                // byteBuffer[i - bufferStart] = readOnlyBuffer.get(i);
                // }
                String s = new String(byteBuffer, StandardCharsets.UTF_8);

                var posOf = s.indexOf(";");
                var city = s.substring(0, posOf);
                var measuredValue = s.substring(posOf + 1);

                var measurementRecord = new MeasurementRecord(city, Double.parseDouble(measuredValue));
                readRecords.add(measurementRecord);
                bufferPosition++;
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return readRecords;
    }

    private static final Function<Map.Entry<String, DoubleSummaryStatistics>, String> MIN_AVG_MAX = s -> String.format("%s=%.1f/%.1f/%.1f", s.getKey(),
            s.getValue().getMin(), s.getValue().getAverage(), s.getValue().getMax());

    public static void main(String[] args) throws IOException {
        var fileAttributeView = Files.getFileAttributeView(MEASUREMENT_FILES, BasicFileAttributeView.class);
        var totalSizeOfFile = fileAttributeView.readAttributes().size();

        var sizePerCore = totalSizeOfFile / Runtime.getRuntime().availableProcessors();
        var numberOfChunks = totalSizeOfFile / sizePerCore;

        List<Positions> positions = new ArrayList<>();

        for (long currentPosition = 0L, endPosition = sizePerCore, counter = 0; counter <= numberOfChunks;) {
            var calculate = calculate(currentPosition, endPosition);
            positions.add(calculate);
            currentPosition = calculate.endPosition() + 1;

            if ((currentPosition + sizePerCore) < totalSizeOfFile) {
                endPosition = currentPosition + sizePerCore;
            }
            else {
                endPosition = totalSizeOfFile;
            }
            counter++;
        }

        positions.stream().forEach(s -> System.out.println(s));
        var resultList = positions.parallelStream()
                .map(CalculateAverage_khmarbaise_v2::readOneChunk)
                .flatMap(List::stream)
                .collect(groupingBy(MeasurementRecord::city, Collectors.summarizingDouble(MeasurementRecord::measuredValue)))
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .map(MIN_AVG_MAX)
                .collect(Collectors.joining(", "));

        System.out.println("{" + resultList + "}");

    }
}
