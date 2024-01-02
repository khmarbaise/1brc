package dev.morling.onebrc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CalculateAverage_khmarbaiseTest {

    @Test
    void name() {
        var line = "Aserbaichan;12,5";
        var posOf = line.indexOf(";");
        var city = line.substring(0, posOf);
        var value = line.substring(posOf + 1);

        System.out.println("posOf = " + posOf);
        System.out.println("city = " + city);
        System.out.println("value = " + value);

    }
}