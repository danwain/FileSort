package uk.ac.cam.daw87.fjava.tick0;

import uk.ac.cam.daw87.fjava.tick0.ExternalSort;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class ExternalSortTest {
    private static final String dir = "/home/dan/Documents/Projects/FileSort/data";
    private static final String[] Hashes = {
            "d41d8cd98f0b24e980998ecf8427e",
            "a54f041a9e15b5f25c463f1db7449",
            "c2cb56f4c5bf656faca0986e7eba38",
            "c1fa1f22fa36d331be4027e683baad6",
            "8d79cbc9a4ecdde112fc91ba625b13c2",
            "1e52ef3b2acef1f831f728dc2d16174d",
            "6b15b255d36ae9c85ccd3475ec11c3",
            "1484c15a27e48931297fb6682ff625",
            "ad4f60f065174cf4f8b15cbb1b17a1bd",
            "32446e5dd58ed5a5d7df2522f0240",
            "435fe88036417d686ad8772c86622ab",
            "c4dacdbc3c2e8ddbb94aac3115e25aa2",
            "3d5293e89244d513abdf94be643c630",
            "468c1c2b4c1b74ddd44ce2ce775fb35c",
            "79d830e4c0efa93801b5d89437f9f3e",
            "c7477d400c36fca5414e0674863ba91",
            "cc80f01b7d2d26042f3286bdeff0d9"
    };


    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {

        String[][] testList = new String[17][];
        for (int i = 1; i <= 17 ; i++) {
            String[] testFile = new String[3];
            testFile[0] = dir + "/" + "test" + i + "a.dat";
            testFile[1] = dir + "/" + "test" + i + "b.dat";
            testFile[2] = Hashes[i - 1];
            testList[i-1] = testFile;
        }

        boolean failed = false;
        long runningTotal = 0;

        int count = 0;


        for (String[] str : testList){
            System.gc();
            long start = System.nanoTime();
            ExternalSort.sort(str[0], str[1]);
            long end = System.nanoTime();
            long total = end - start;
            runningTotal += total;
            //System.out.println("Time: " + total);
            if (!str[2].equals(ExternalSort.checkSum(str[0]))) {
                System.out.println("Failed " + str[0]);
                failed = true;
            } else {
                System.out.println("Passed " + str[0]);
            }
            count++;
            if (count == -1)
                break;
        }
        System.out.println("The overall status is " + (!failed));
        System.out.println("Total time is " + runningTotal / 1000000000 + " seconds");
    }
}
