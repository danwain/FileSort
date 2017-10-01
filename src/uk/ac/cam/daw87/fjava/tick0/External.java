package uk.ac.cam.daw87.fjava.tick0;

import uk.ac.cam.daw87.fjava.tick0.helpers.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public final class External {
    private final int size; //Array Size
    private final FileChannel f1;
    private final FileChannel f2;
    private final int Total_ints;
    private final int fileSize;
    private final int groups;

    public External(int size, String p1, String p2) throws FileNotFoundException, IOException {
        this.size = size;
        this.f1 = new RandomAccessFile(p1,"rw").getChannel();
        this.fileSize = (int) f1.size();
        this.f2 = new RandomAccessFile(p2,"rw").getChannel();
        this.Total_ints = (int) fileSize / 4;
        this.groups = Helper.roundUp(Total_ints, size);
    }

    public final void sort() throws IOException, IllegalArgumentException{
        if (this.fileSize <= this.size) {
            long start = System.nanoTime();
            SortInMemory(f1, fileSize, Total_ints);
            long end = System.nanoTime();
            //System.out.println("Ram took " + (end - start) + " nano seconds");
        } else {
            long start = System.nanoTime();
            //System.out.println(Helper.FileToString(new RandomAccessFile(from,"rw")));
            initialSort(f1, f2, ByteBuffer.allocate(size * 4));
            long end = System.nanoTime();
            System.out.println((end - start) + " nano seconds (initial)");
            //System.out.println(size);
            //System.out.println(Helper.FileToString(new RandomAccessFile(to,"rw")));
            start = System.nanoTime();
            merge();
            end = System.nanoTime();
            System.out.println((end - start) + " nano seconds (merge)");
            //System.out.println(Helper.FileToString(new RandomAccessFile(from,"r")));
        }
    }

    private static void SortInMemory(FileChannel f, int fileSize, int totalInts) throws IOException{
        //System.out.print("in Ram ");
        if (totalInts <= 1)
            return;
        f.position(0);
        ByteBuffer buffer = ByteBuffer.allocate(fileSize);
        int read = f.read(buffer);
        buffer.position(0);
        Sorters.quickSort(buffer.asIntBuffer(), 0, totalInts);
        buffer.position(0);
        f.position(0);
        f.write(buffer);
    }

    public void merge() throws IOException{
        f2.position(0);
        Map<Integer, Position> lookup = new HashMap<>();
        for (int i = 0; i < groups - 1 ; i++) {
            Position p = new Position(i * size, size, f2);
            lookup.put(p.Start_Position, p);
        }
        Position last_position = new Position(lookup.size()*size,Total_ints - (lookup.size()*size), f2);
        lookup.put(last_position.Start_Position, last_position);

        int amount_in_each = (size * 3) / groups;

        BinaryHeap heap = new BinaryHeap(lookup.size() * amount_in_each);

        ByteBuffer write = ByteBuffer.allocate(size);
        ByteBuffer buffer = ByteBuffer.allocate(amount_in_each * 4);

        for (Position p : lookup.values()){
            int length = p.getNext(amount_in_each, buffer);
            for (int i = 0; i < length; i++){
                heap.insert(buffer.getInt(i*4), p.Start_Position);
            }
            assert p.amountInHeap == 0;
            p.amountInHeap = length;
        }
        heap.heapifyUP();

        f1.position(0);
        while (!heap.isEmpty()){
            if (write.position() + 4 > write.capacity()){
                write.flip();
                f1.write(write);
                write.clear();
            }
            int[] min = heap.getMin();
            Position position = lookup.get(min[1]);
            if (position.amountInHeap <= 0) {
                System.out.println(position);
                System.out.println(min[0]);
                System.out.println(position.amountInHeap);
            }
            assert position.amountInHeap > 0;
            if (position.amountInHeap == 1){
                int length = position.getNext(amount_in_each, buffer);
                if (length != 0){
                    for (int i = 0; i < length; i++) {
                        heap.insert(buffer.getInt(i * 4), position.Start_Position);
                    }
                    heap.heapifyUP();
                    position.amountInHeap = length;
                } else {
                    position.amountInHeap = 0;
                }
            } else {
                position.amountInHeap--;
            }
            write.putInt(min[0]);
        }
        write.flip();
        f1.write(write);
    }

    private  static void initialSort(FileChannel f1, FileChannel f2, ByteBuffer buffer) throws IOException {
        //TODO: Sort better (maybe radix/bucket)
        //f1.position(0);
        //f2.position(0);
        int length;
        long start, end;
        long total_time = 0;
        while (true) {
            buffer.clear();
            length = f1.read(buffer);
            if (length <= 0)
                break;
            buffer.position(0);
            start = System.nanoTime();
            Sorters.quickSort(buffer.asIntBuffer(), 0, length / 4);
            end = System.nanoTime();
            total_time += (end - start);
            buffer.position(0);
            buffer.limit(length);
            f2.write(buffer);
        }
        System.out.println(total_time + " QuickSort");
    }
}
