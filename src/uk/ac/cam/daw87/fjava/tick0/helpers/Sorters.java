package uk.ac.cam.daw87.fjava.tick0.helpers;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public class Sorters {
    public static void ByteQuickSort(ByteBuffer buffer, int start, int end){
        assert buffer != null;
        assert buffer.capacity() % 4 == 0;
        assert (end - start) % 4 == 0;
        if (end - start <= 4)
            return;

        int m = BytePartition(buffer, start, end);
        ByteQuickSort(buffer, start, m);
        ByteQuickSort(buffer, m+4, end);
    }

    private static int BytePartition(ByteBuffer buffer, int start, int end){
        int il = start;
        int ir = end - 4;
        int pivot = buffer.getInt(ir);
        int temp;
        while (ir > il){
            if (buffer.getInt(ir - 4) > pivot){
                buffer.putInt(ir, buffer.getInt(ir - 4));
                ir-=4;
            } else {
                temp = buffer.getInt(ir - 4);
                buffer.putInt(ir - 4, buffer.getInt(il));
                buffer.putInt(il, temp);
                il+=4;
            }
        }
        buffer.putInt(il,pivot);
        return il;
    }

    public static void quickSort(ByteBuffer buffer){
        buffer.position(0);
        IntBuffer i = buffer.asIntBuffer();
        quickSort(i,0,i.capacity());
    }

    private static boolean probablysorted(IntBuffer buffer, int start, int end){
        int first = buffer.get(start);
        int second = buffer.get(start + 1);
        int thrid = buffer.get(start + 2);
        int fourth = buffer.get(start + 3);
        return first <= second && second <= thrid && thrid <= fourth;
    }

    public static void quickSort(IntBuffer buffer, int start, int end){
        int length = end - start;
        if (length < 7 || probablysorted(buffer, start, end)){
            insertSort(buffer, start, end);
        } else {
            quickSortInternal(buffer, start, end);
        }
    }

    private static void quickSortInternal(IntBuffer buffer, int start, int end){
        assert buffer != null;
        if (end - start < 7 || probablysorted(buffer, start, end)) {
            insertSort(buffer, start, end);
        } else {
            int m = partition(buffer, start, end);
            quickSortInternal(buffer, start, m);
            quickSortInternal(buffer, m + 1, end);
        }
    }

    private static void swapp(IntBuffer buffer, int a, int b){
        int temp = buffer.get(a);
        buffer.put(a, buffer.get(b));
        buffer.put(b, temp);
    }

    private static int median(IntBuffer buffer, int start, int end){
        int first = buffer.get(start);
        int last = buffer.get(end - 1);
        int middle = buffer.get((start + end - 1) >> 1);

        int x = first-last;
        int y = last-middle;
        int z = first-middle;
        if (x*y > 0){
            return end - 1;
        } else if(x*z > 0) {
            return (start + end - 1) >> 1;
        } else {
            return start;
        }
    }

    private static int partition(IntBuffer buffer, int start, int end){
        int il = start;
        int ir = end - 1;
        int average = median(buffer, start, end);
        swapp(buffer, average, ir);
        average = ir;
        //assert (average <= ir && average >= il);
        int pivot = buffer.get(average);
        int temp;
        while (ir > il){
            temp = buffer.get(ir - 1);
            if (temp > pivot){
                buffer.put(ir, temp);
                ir--;
            } else {
                buffer.put(ir - 1, buffer.get(il));
                buffer.put(il, temp);
                il++;
            }
        }
        buffer.put(il,pivot);
        return il;
    }

    private static void insertSort(IntBuffer buffer, int start, int end){
        int temp;
        int k;
        for (int i = start + 1; i < end; i++) {
            temp = buffer.get(i);
            for (k = i - 1; k >= 0 && temp < buffer.get(k); k--){
                buffer.put(k + 1, buffer.get(k));
            }
            buffer.put(k + 1, temp);
        }
    }


    public static void RadixSort(byte[] a, int start, int end) {
        final int R = 1 << 8;    // each bytes is between 0 and 255
        final int MASK = R - 1;              // 0xFF
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

            // for most significant byte, 0x80-0xFF comes before 0x00-0x7F
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
            for (int i = 0; i < n; i++)
                a[i + start] = aux[i];
        }
    }
}
