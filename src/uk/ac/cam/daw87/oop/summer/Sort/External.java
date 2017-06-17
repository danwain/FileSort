package uk.ac.cam.daw87.oop.summer.Sort;

import uk.ac.cam.daw87.oop.summer.Helper;
import uk.ac.cam.daw87.oop.summer.Sort.helpers.Number;
import uk.ac.cam.daw87.oop.summer.Sort.helpers.Position;

import java.io.*;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public class External {
    private final int size; //Array Size
    private final RandomAccessFile f1;
    private final RandomAccessFile f2;
    private final int Total_ints;
    private final int groups;
    private final Set<Position> positions;

    public External(int size, String p1, String p2) throws FileNotFoundException, IOException{
        this.size = size;
        this.f1 = new RandomAccessFile(p1,"rw");
        this.f2 = new RandomAccessFile(p2,"rw");
        this.Total_ints = (int) f1.length() / 4;
        this.groups = Helper.roundUp(Total_ints, size);
        this.positions = new LinkedHashSet<>();
    }

    public void sort() throws IOException, IllegalArgumentException{
        //System.out.println(Helper.FileToString(f1));
        initialSort(f1.getChannel(),f2.getChannel(), ByteBuffer.allocate(size*4));
        //System.out.println(Helper.FileToString(f2));
        //merge();
        //System.out.println(Helper.FileToString(f1));
    }

    private void merge() throws IOException{
        f2.seek(0);
        for (int i = 0; i < groups - 1 ; i++)
            positions.add(new Position(i*size,size,f2.getChannel()));
        positions.add(new Position(positions.size()*size,Total_ints - (positions.size()*size),f2.getChannel()));

        Queue<Number> queue = new PriorityQueue<>();
        Map<Position, Integer> amounts = new HashMap<>();
        int[] write = new int[size / 2];
        int amount_in_each = (size / 2) / groups;
        if (amount_in_each == 0)
            amount_in_each = 1;
        ByteBuffer buffer = ByteBuffer.allocate(amount_in_each * 4);
        for (Position p : positions){
            //Set<Number> toAdd = p.getNext(amount_in_each);
            int length = p.getNext(amount_in_each, buffer);
            for (int i = 0; i < length; i++)
                queue.add(new Number(buffer.getInt(i*4),p));
            amounts.put(p,length);
        }
        f1.seek(0);
        int index = 0;
        while (!queue.isEmpty()){
            if (index == write.length){
                for (int i : write)
                    f1.writeInt(i);
                index = 0;
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
            write[index] = n.number;
            index++;
        }
        for (int i = 0; i < index ; i++)
            f1.writeInt(write[i]);
    }

    private void initialSort(FileChannel f1, FileChannel f2, ByteBuffer buffer) throws IOException{
        //TODO: Sort better without creating an array ie using only the buffer then write the buffer.
        f1.position(0);
        f2.position(0);
        int length;
        while (true){
            buffer.position(0);
            length = f1.read(buffer);
            if (length <= 0)
                break;
            int[] temp = new int[length / 4];
            for (int i = 0; i < length / 4; i++)
                temp[i] = buffer.getInt(i*4);
            Arrays.sort(temp);
            int start = buffer.capacity() - length;
            buffer.position(start);
            for (int i : temp)
                buffer.putInt(i);
            buffer.position(start);
            f2.write(buffer);
        }
    }
}
