package uk.ac.cam.daw87.fjava.tick0.helpers;


public class Sorters {

    public static void RadixSort(byte[] a, int start, int end) {
        final int R = 256;    // each bytes is between 0 and 255
        final int MASK = 255;              // 0xFF
        final int w = 4;  // each int is 4 bytes

        int n = end - start;
        byte[] aux = new byte[n];

        for (int d = w-1; d >= 0; d--) {

            // compute frequency counts
            int[] count = new int[R+1];
            for (int i = d; i < n; i+=4) {
                int c = a[i + start] & MASK;
                count[c + 1]++;
            }

            // compute cumulates
            for (int r = 0; r < R; r++)
                count[r+1] += count[r];

            // for most significant byte due to being signed
            if (d == 0) {
                int shift1 = count[R] - count[R/2];
                int shift2 = count[R/2];
                for (int r = 0; r < R/2; r++)
                    count[r] += shift1;
                for (int r = R/2; r < R; r++)
                    count[r] -= shift2;
            }

            // move data
            for (int i = d; i < n; i+=4) {
                int c = a[i + start] & MASK;
                for (int j = 0; j < 4 ; j++) {
                    aux[count[c] * 4 + j] = a[i + j - d + start];
                }
                count[c]++;
            }

            // copy back
            System.arraycopy(aux, 0, a, start, n);
        }
    }
}
