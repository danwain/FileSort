package uk.ac.cam.daw87.fjava.tick0.helpers;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Random;
import java.util.SplittableRandom;

public class SortersTest {
    private static final int[] test = {26,27,28,28,16};

    public static void main(String[] args) throws IOException{
        //IntBuffer buffer = IntBuffer.wrap(test);
        //System.out.println(Arrays.toString(test));
        //Sorters.quickSort(buffer, 0, test.length);
        //System.out.println(Arrays.toString(test));

        //testRandom(1000, 100);
        test6a();
    }

    public static ByteBuffer clone(ByteBuffer original) {
        ByteBuffer clone = ByteBuffer.allocate(original.capacity());
        original.rewind();//copy from the beginning
        clone.put(original);
        original.rewind();
        clone.flip();
        return clone;
    }

    private static void testRandom(int length, int amount){
        for (int i = 0; i < amount; i++) {
            int[] toTest = RandomData(length);
            int[] Check = toTest.clone();
            Arrays.sort(Check);
            //Sorters.quickSort(IntBuffer.wrap(toTest), 0, toTest.length);
            assert Arrays.equals(Check, toTest);
        }
    }

    private static void test6a() throws IOException{
        FileChannel file = new RandomAccessFile("/home/dan/Documents/Projects/FileSort/data/test6a.dat", "rw").getChannel();
        ByteBuffer buffer = ByteBuffer.allocate((int) file.size());//ByteBuffer.allocate((int) file.size());
        int read = file.read(buffer);
        System.out.println(read);
        buffer.position(0);
        IntBuffer intBuffer = buffer.asIntBuffer();
        ByteBuffer test2 = clone(buffer);
        int end = intBuffer.capacity();
        //Sorters.quickSort(intBuffer, 0, end);
        Sorters.RadixSort(test2.array(), 0, end * 4);
        for (int i = 0; i < intBuffer.capacity(); i++) {
            System.out.print(intBuffer.get(i) + ",");
        }
        System.out.println();
        IntBuffer intBuffer1 = test2.asIntBuffer();
        for (int i = 0; i < intBuffer1.capacity() ; i++) {
            System.out.print(intBuffer1.get(i) + ",");
        }
        System.out.println();
        System.out.println(Arrays.equals(test2.array(), buffer.array()));
    }


    private static int[] RandomData(int length){
        Random r = new Random();
        SplittableRandom s = new SplittableRandom();
        int[] out = new int[length];
        for (int i = 0; i < length ; i++) {
            int next = s.nextInt();
            out[i] = next;
        }
        return out;
    }
}
