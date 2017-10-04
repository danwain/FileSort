package uk.ac.cam.daw87.fjava.tick0;

import uk.ac.cam.daw87.fjava.tick0.helpers.*;

import java.awt.*;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public final class External {
    private final static int InitialSortSize = 756490;//(10000000 / 4) - 1000000;
    private final static int InMemorySort = 5;//10000000 - 500;
    private final static int InMemoryRadix = 1000000;
    private final static int MaxHeapSize = 240000;//((((10000000 / 4) - 3000000) / 5) * 4 ) / 2;
    private final static int WriterSize = 3000;//(((10000000 / 4) - 3000000) / 5);

    private final FileChannel f1;
    private final FileChannel f2;
    private final int Total_ints;
    private final int fileSize;
    private final int groups;

    public External(String p1, String p2) throws IOException {
        this.f1 = new RandomAccessFile(p1,"rw").getChannel();
        this.fileSize = (int) f1.size();
        this.f2 = new RandomAccessFile(p2,"rw").getChannel();
        this.Total_ints = fileSize / 4;
        this.groups = Helper.roundUp(Total_ints, InitialSortSize);
        System.out.println("File Size: " + fileSize);
    }

    public final void sort() throws IOException, IllegalArgumentException{
        if (this.fileSize <= 4){
            return;
        } else if (this.fileSize <= InMemoryRadix){
            SortInMemory(f1, fileSize, Total_ints, true);
        } else if (this.fileSize <= InMemorySort) {
            //System.out.println("Ram sort");
            //long start = System.nanoTime();
            SortInMemory(f1, fileSize, Total_ints, false);
            //long end = System.nanoTime();
            //System.out.println("Ram took " + (end - start) + " nano seconds");
        } else {
            long start = System.nanoTime();
            //System.out.println(Helper.FileToString(new RandomAccessFile(from,"rw")));
            initialSort(f1, f2);
            long end = System.nanoTime();
            System.out.println((end - start) + " nano seconds (initial)");
            //System.out.println(size);
            //System.out.println(Helper.FileToString(new RandomAccessFile(to,"rw")));
            start = System.nanoTime();
            merge(f1, f2, groups, Total_ints, fileSize);
            end = System.nanoTime();
            System.out.println((end - start) + " nano seconds (merge)");
            //System.out.println(Helper.FileToString(new RandomAccessFile(from,"r")));
        }
    }

    private static void SortInMemory(FileChannel f, int fileSize, int totalInts, boolean radix) throws IOException{
        System.out.println("in Ram ");
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

    private static void merge(FileChannel f1, FileChannel f2, int groups, int TotalInts, int fileSize) throws IOException{
        f2.position(0);
        f1.position(0);
        long start, end;

        start = System.nanoTime();
        int[][] lookup = new int[groups][3];
        for (int i = 0; i < groups - 1; i++) {
            lookup[i][0] = (i * InitialSortSize + InitialSortSize) * 4;
            lookup[i][1] = i * InitialSortSize * 4;
            //lookup[i][2] = 0;
        }
        lookup[groups - 1][0] = fileSize;
        lookup[groups - 1][1] = (groups - 1) * InitialSortSize * 4;
        end = System.nanoTime();
        System.out.println("Lookup setup: " + (end - start));

        start = System.nanoTime();
        int amount_in_each = MaxHeapSize / groups;

        BinaryHeap heap = new BinaryHeap(groups * amount_in_each);

        ByteBuffer write = ByteBuffer.allocate(WriterSize);
        ByteBuffer buffer = ByteBuffer.allocate(amount_in_each * 4);
        IntBuffer intBuffer = buffer.asIntBuffer();
        for (int k = 0; k < lookup.length; k++) {
            int length = readBuffer(buffer, f2, lookup, k, amount_in_each);
            for (int i = 0; i < length; i++){
                heap.addNoHeaify(intBuffer.get(i), k);
            }
            assert lookup[k][2] == 0;
            lookup[k][2] = length;
        }
        heap.build();
        end = System.nanoTime();

        System.out.println("Heap setup : " + (end - start));


        while (!heap.isEmpty()){
            if (write.position() + 4 > WriterSize){
                write.flip();
                f1.write(write);
                write.clear();
            }
            int[] min = heap.getMin();
            assert lookup[min[1]][2] > 0; // amount in heap
            if (lookup[min[1]][2] == 1){
                int length = readBuffer(buffer, f2, lookup, min[1], amount_in_each);
                for (int i = 0; i < length; i++) {
                    heap.insert(intBuffer.get(i), min[1]);
                }
                lookup[min[1]][2] = length; // amount in Heap
            } else {
                lookup[min[1]][2]--;
            }
            write.putInt(min[0]);
        }
        write.flip();
        f1.write(write);
    }

    private static int readBuffer(ByteBuffer buffer, FileChannel file, int[][] positions, int i, int toRead) throws IOException{
        int end = positions[i][0];
        int index = positions[i][1];
        if (index == end)
            return 0;
        buffer.position(0);
        file.position(index);
        int length = Math.min(toRead * 4, end - index);
        buffer.limit(length);
        int read = file.read(buffer);
        assert (read == length);
        index+=length;

        positions[i][1] = index;
        return length / 4;
    }

    private  static void initialSort(FileChannel f1, FileChannel f2) throws IOException {
        //TODO: Sort better (maybe radix/bucket)
        //f1.position(0);
        //f2.position(0);
        ByteBuffer buffer = ByteBuffer.allocate(InitialSortSize * 4);
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
