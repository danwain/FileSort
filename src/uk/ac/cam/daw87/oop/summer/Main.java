package uk.ac.cam.daw87.oop.summer;

import uk.ac.cam.daw87.oop.summer.Sort.External;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException, IllegalArgumentException {
        String f1 = "data/test10a.dat";
        String f2 = "data/test10b.dat";
        //Helper.CreateData(f1);
        External e = new External(100,f1,f2);
        e.sort();
    }
}
