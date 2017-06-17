package uk.ac.cam.daw87.oop.summer;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

public class Helper {
    public static String ArrayToString(int[] toPrint){
        StringBuilder string = new StringBuilder();
        for (int i: toPrint)
            string.append(i).append(", ");
        string.delete(string.length() - 2, string.length());
        return string.toString();
    }


    public static int lg(int input){
        boolean dirty = false;
        int result = 0;

        while (input > 1){
            if (!dirty && (input % 2 !=0))
                dirty = true;
            input = input >> 1;
            result++;
        }
        if (dirty)
            result++;
        return result;
    }

    //Only take positive
    public static int roundUp(int num, int divisor) {
        return (num + divisor - 1) / divisor;
    }

    public static String FileToString(RandomAccessFile r) throws IOException{
        StringBuilder s = new StringBuilder();
        r.seek(0);
        for (int i = 0; i < (r.length() / 4); i++)
            s.append(r.readInt()).append(", ");
        if (s.length() > 2)
            s.delete(s.length() - 2, s.length());
        return s.toString();
    }

    private static int[] FileToArray(RandomAccessFile f) throws IOException{
        int[] array = new int[(int) f.length() / 4];
        f.seek(0);
        for (int i = 0; i < f.length() / 4 ; i++)
            array[i] = f.readInt();
        return array;
    }

    public static boolean isSorted(int[] array){
        for (int i = 0; i < array.length - 1; i++) {
            if (array[i] > array[i+1])
                return false;
        }
        return true;
    }

    public static boolean equal(int[] a1, int[] a2){
        if (a1.length != a2.length)
            return false;

        for (int i = 0; i < a1.length ; i++) {
            if (a1[i] != a2[i])
                return false;
        }
        return true;
    }

    public static void CreateData(String location) throws IOException{
        RandomAccessFile f = new RandomAccessFile(location,"rw");
        int[] test = {0,5,2,-5,10};
        for (int i : test) {
            f.writeInt(i);
        }
        f.close();
    }
}
