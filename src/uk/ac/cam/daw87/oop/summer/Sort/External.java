package uk.ac.cam.daw87.oop.summer.Sort;

import uk.ac.cam.daw87.oop.summer.Helper;
import uk.ac.cam.daw87.oop.summer.Sort.helpers.Number;
import uk.ac.cam.daw87.oop.summer.Sort.helpers.Position;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public class External {
    private final int size; //Array Size
    private final String to;
    private final String from;
    private final FileChannel f1;
    private final FileChannel f2;
    private final int Total_ints;
    private final int groups;
    private final Set<Position> positions;

    public External(int size, String p1, String p2) throws FileNotFoundException, IOException{
        this.size = size;
        this.from = p1;
        this.to = p2;
        this.f1 = new RandomAccessFile(p1,"rw").getChannel();
        this.f2 = new RandomAccessFile(p2,"rw").getChannel();
        this.Total_ints = (int) f1.size() / 4;
        this.groups = Helper.roundUp(Total_ints, size);
        this.positions = new LinkedHashSet<>();
    }

    public void sort() throws IOException, IllegalArgumentException{
        //System.out.println(Helper.FileToString(new RandomAccessFile(from,"rw")));
        //initialSort(f1,f2, ByteBuffer.allocate(size*4));
        //System.out.println(Helper.FileToString(new RandomAccessFile(to,"rw")));
        merge();
        //System.out.println(Helper.FileToString(new RandomAccessFile(from,"r")));
    }

    public void merge() throws IOException{
        f2.position(0);
        for (int i = 0; i < groups - 1 ; i++)
            positions.add(new Position(i*size,size,f2));
        positions.add(new Position(positions.size()*size,Total_ints - (positions.size()*size),f2));

        Queue<Number> queue = new PriorityQueue<>();
        Map<Position, Integer> amounts = new HashMap<>();
        ByteBuffer write = ByteBuffer.allocate(1+(size / 2) - (size / 2) % 4);
        int amount_in_each = (size / 2) / groups;
        if (amount_in_each == 0)
            amount_in_each = 1;
        ByteBuffer buffer = ByteBuffer.allocate(amount_in_each * 4);
        for (Position p : positions){
            int length = p.getNext(amount_in_each, buffer);
            for (int i = 0; i < length; i++)
                queue.add(new Number(buffer.getInt(i*4),p));
            amounts.put(p,length);
        }
        f1.position(0);
        while (!queue.isEmpty()){
            if (write.position() + 4 > write.capacity()){
                write.flip();
                f1.write(write);
                write.clear();
            }
            Number n = queue.poll();
            assert amounts.get(n.group) > 0;
            if (amounts.get(n.group) == 1){
                int length = n.group.getNext(amount_in_each, buffer);
                if (length != 0){
                    for (int i = 0; i < length; i++)
                        queue.add(new Number(buffer.getInt(i*4),n.group));
                    amounts.put(n.group,length);
                } else amounts.put(n.group,0);
            } else {
                amounts.put(n.group, amounts.get(n.group) - 1);
            }
            write.putInt(n.number);
        }
        write.flip();
        f1.write(write);
    }

    private void initialSort(FileChannel f1, FileChannel f2, ByteBuffer buffer) throws IOException{
        //TODO: Sort better (maybe radix/bucket)
        f1.position(0);
        f2.position(0);
        int length;
        while (true){
            buffer.clear();
            length = f1.read(buffer);
            if (length <= 0)
                break;
            buffer.position(0);
            BufferSortQuickSort.quickSort(buffer.asIntBuffer(),0,length/4);
            buffer.position(0);
            buffer.limit(length);
            f2.write(buffer);
        }
    }
}
