package uk.ac.cam.daw87.fjava.tick0.helpers;

public class Helper {
    public static int roundUp(int num, int divisor) {
        return (num + divisor - 1) / divisor;
    }
}
