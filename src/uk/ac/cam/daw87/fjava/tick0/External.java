package uk.ac.cam.daw87.fjava.tick0;

import uk.ac.cam.daw87.fjava.tick0.helpers.*;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.Future;

public final class External {
    private final static int InitialSortSize = 756490;//(10000000 / 4) - 1000000;
    private final static int InMemorySort =  1;//10000000 - 500;
    private final static int InMemoryRadix = 1000000;
    private final static int MaxHeapSize = 700000;//244000;//((((10000000 / 4) - 3000000) / 5) * 4 ) / 2;
    private final static int WriterSize = 6000;//(((10000000 / 4) - 3000000) / 5);

    public static void sort(String p1, String p2) throws IOException, IllegalArgumentException{
        FileChannel f1 = new RandomAccessFile(p1,"rw").getChannel();
        FileChannel f2 = new RandomAccessFile(p2, "rw").getChannel();
        AsynchronousFileChannel f2a = AsynchronousFileChannel.open(Paths.get(p2));
        final int fileSize = (int) f1.size();
        final int totalInts = fileSize >> 2;

        if (fileSize <= 4){
            return;
        } else if (fileSize <= InMemoryRadix){
            SortInMemory(f1, fileSize, totalInts, true);
        } else if (fileSize <= InMemorySort) {
            //System.out.println("Ram sort");
            //long start = System.nanoTime();
            SortInMemory(f1, fileSize, totalInts, false);
            //long end = System.nanoTime();
            //System.out.println("Ram took " + (end - start) + " nano seconds");
        } else {
            //long start = System.nanoTime();
            //System.out.println(Helper.FileToString(new RandomAccessFile(from,"rw")));
            initialSort(f1, f2);
            //long end = System.nanoTime();
            //System.out.println("Initial sort          : " + (end - start));
            //System.out.println(size);
            //System.out.println(Helper.FileToString(new RandomAccessFile(to,"rw")));
            //start = System.nanoTime();
            int groups = Helper.roundUp(totalInts, InitialSortSize);
            merge(f1, f2a, groups, fileSize, p1, p2);
            //end = System.nanoTime();
            //System.out.println("Merge                 : " + (end - start));
            //System.out.println(Helper.FileToString(new RandomAccessFile(from,"r")));
        }
    }

    private static void SortInMemory(FileChannel f, int fileSize, int totalInts, boolean radix) throws IOException{
        //System.out.println("in Ram ");
        if (totalInts <= 1)
            return;
        f.position(0);
        ByteBuffer buffer = ByteBuffer.allocate(fileSize);
        int read = f.read(buffer);
        assert read == totalInts * 4;
        buffer.position(0);
        if (radix){
            Sorters.RadixSort(buffer.array(), 0, fileSize);
        } else {
            Sorters.quickSort(buffer.asIntBuffer(), 0, totalInts);
        }
        buffer.position(0);
        f.position(0);
        f.write(buffer);
    }

    private static void merge(FileChannel f1, AsynchronousFileChannel f2, int groups, int fileSize, String p1, String p2) throws IOException{
        //long start, end;
        //System.out.println(groups);
        f1.position(0);
        assert groups >= 1;

        //start = System.nanoTime();
        final int amount_in_each = MaxHeapSize / groups;
        //amount_in_each = 1;
        int[][] lookup = new int[groups][2];
        ByteBuffer[][] readCache = new ByteBuffer[groups][2];
        Future<Integer> [] readDone = new Future[groups];
        for (int i = 0; i < groups - 1; i++) {
            readCache[i][0] = ByteBuffer.allocate(amount_in_each * 4);
            readCache[i][1] = ByteBuffer.allocate(amount_in_each * 4);
        }
        readCache[groups - 1][0] = ByteBuffer.allocate((Math.min(fileSize - (groups - 1) * InitialSortSize * 4, amount_in_each) / 4) * 4);
        readCache[groups - 1][1] = ByteBuffer.allocate((Math.min(fileSize - (groups - 1) * InitialSortSize * 4, amount_in_each) / 4) * 4);


        for (int i = 0; i < groups - 1; i++) {
            lookup[i][0] = (i + 1) * InitialSortSize * 4;
            lookup[i][1] = i * InitialSortSize * 4;
        }
        lookup[groups - 1][0] = fileSize;
        lookup[groups - 1][1] = (groups - 1) * InitialSortSize * 4;
        //end = System.nanoTime();
        //System.out.println("Lookup setup          : " + (end - start));

        //start = System.nanoTime();

        assert amount_in_each > 0;

        //BinaryHeap heap = new BinaryHeap(MaxHeapSize);
        int HeapSize = 0;
        final int[][] Heap = new int[groups][2];
        final int[] temp = new int[2];
        final int[] returnTemp = new int[2];

        ByteBuffer write = ByteBuffer.allocate(WriterSize);
        for (int k = 0; k < lookup.length; k++) {
            boolean test = readBuffer(f2, lookup, k, amount_in_each, readCache, readDone);
            assert test;
            BinaryHeap.addNoHeaify(Heap, HeapSize, readCache[k][0].getInt(), k);
            HeapSize++;
        }
        //HeapSize += lookup.length;
        BinaryHeap.build(Heap, HeapSize, temp);
        //end = System.nanoTime();

        //System.out.println("Heap setup            : " + (end - start));

        //long[] starts = new long[3];
        //long[] ends = new long[3];
        //long[] totals = new long[3];
        assert HeapSize > 0;
        while (!BinaryHeap.isEmpty(HeapSize)){
            //starts[0] = System.nanoTime();
            if (write.position() + 4 > WriterSize){
                write.flip();
                f1.write(write);
                write.clear();
            }
            //ends[0] = System.nanoTime();
            //totals[0] += ends[0] - starts[0];
            //starts[1] = System.nanoTime();
            BinaryHeap.getMin(Heap, HeapSize, returnTemp, temp);
            HeapSize--;

            if (readCache[returnTemp[1]][0].remaining() >= 4 || readBuffer(f2, lookup, returnTemp[1], amount_in_each, readCache, readDone)) {
                assert readCache[returnTemp[1]][0].remaining() >= 4;
                BinaryHeap.insert(Heap, HeapSize, readCache[returnTemp[1]][0].getInt(), returnTemp[1], temp);
                HeapSize++;
            }
            //ends[2] = System.nanoTime();
            //totals[2] += ends[2] - starts[2];
            write.putInt(returnTemp[0]);
        }
        //System.out.println("Time spent writing    : " + totals[0]);
        //System.out.println("Time spent Finding min: " + totals[1]);
        //System.out.println("Time spent Reading    : " + totals[2]);
        assert checkAllwrited(lookup, fileSize);
        //assert checkBuffers(readCache);
        write.flip();
        f1.write(write);
    }

    private static boolean checkBuffers(ByteBuffer[] buffers){
        for (ByteBuffer b : buffers){
            if (b.limit() != 0)
                return false;
        }
        return true;
    }


    private static boolean checkAllwrited(int[][] lookup, int filesize){
        for (int i = 0; i < lookup.length - 1; i++) {
            if (lookup[i][1] != lookup[i][0])
                return false;
        }
        if (lookup[lookup.length - 1][0] == filesize) {
            return true;
        } else {
            return false;
        }
    }

    private static void arrayMerge(FileChannel f1, FileChannel f2, int totalInts) throws IOException{
        assert false;
        int b1start = 0;
        int b2start = 0;
        int b1end, b2end;
        int Part1Index = 0;
        int Part2Index = InitialSortSize * 4;
        final ByteBuffer b1 = ByteBuffer.allocate(MaxHeapSize * 4);
        final ByteBuffer b2 = ByteBuffer.allocate(MaxHeapSize * 4);
        f2.position(0);
        f1.position(0);
        b1end = f2.read(b1);
        f2.position(Part2Index);
        b2end = f2.read(b2);

        final ByteBuffer write = ByteBuffer.allocate(WriterSize);

        while (Part1Index < InitialSortSize * 4 && Part2Index < totalInts * 4){
            if (write.position() + 4 > WriterSize){
                write.flip();
                f1.write(write);
                write.clear();
            }
            assert b1start <= b1end;
            assert b2start <= b2end;
            if (b1start == b1end){
                b1.clear();
                b1start = 0;
                f2.position(Part1Index);
                b1end = f2.read(b1);
                b1.limit(b1end);
            } else if (b2start == b2end) {
                b2.clear();
                b2start = 0;
                f2.position(Part2Index);
                b2end = f2.read(b2);
                b2.limit(b2end);
            }
            if (b1.getInt(b1start) < b2.getInt(b2start)){
                write.putInt(b1.getInt(b1start));
                b1start+=4;
                Part1Index+=4;
            } else {
                write.putInt(b2.getInt(b2start));
                b2start+=4;
                Part2Index+=4;
            }
        }
        write.flip();
        f1.write(write);
        b2.position(b2start);
        b1.position(b1start);
        if (Part1Index < InitialSortSize) {
            f1.write(b1);
        } else if (Part2Index < totalInts * 4){
            f1.write(b2);
        }
    }

    private static boolean readBuffer(AsynchronousFileChannel file, int[][] positions, int i, int toRead, ByteBuffer[][] readCache, Future<Integer>[] readFutures) throws IOException{
        final int end = positions[i][0];
        int index = positions[i][1];
        final int length = Math.min(Math.min(toRead << 2, end - index), readCache[i][0].capacity());
        assert (length / 4) * 4 == length;
        assert length >= 0;
        if (readFutures[i] == null){
            readCache[i][1].clear();
            readCache[i][1].limit(length);
            readFutures[i] = file.read(readCache[i][1], index);
            index += length;
            positions[i][1] = index;
        }
        while (!readFutures[i].isDone()) {
            //pass
        }
        assert readFutures[i].isDone();
        ByteBuffer temp = readCache[i][1];
        readCache[i][1] = readCache[i][0];
        readCache[i][0] = temp;
        readCache[i][0].position(0);
        if (index == end) {
            assert length == 0;
            readCache[i][1].limit(0);
        } else {
            readCache[i][1].clear();
            readCache[i][1].limit(length);
            readFutures[i] = file.read(readCache[i][1], index);
            index += length;
        }

        positions[i][1] = index;
        assert (readCache[i][0].limit() / 4) * 4 == readCache[i][0].limit();
        return readCache[i][0].limit() != 0;
    }

    private  static void initialSort(FileChannel f1, FileChannel f2) throws IOException {
        //TODO: Sort better (maybe radix/bucket)
        //f1.position(0);
        //f2.position(0);
        ByteBuffer buffer = ByteBuffer.allocate(InitialSortSize << 2);
        //IntBuffer intBuffer = buffer.asIntBuffer();
        byte[] array = buffer.array();
        int length;
        while (true) {
            buffer.clear();
            length = f1.read(buffer);
            if (length <= 0)
                break;
            buffer.position(0);

            //Sorters.quickSort(intBuffer, 0, length / 4);
            Sorters.RadixSort(array, 0, length);
            buffer.position(0);
            buffer.limit(length);
            f2.write(buffer);
        }
    }
}
