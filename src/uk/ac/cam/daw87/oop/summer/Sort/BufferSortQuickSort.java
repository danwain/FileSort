package uk.ac.cam.daw87.oop.summer.Sort;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public class BufferSortQuickSort {
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


    public static void quickSort(IntBuffer buffer, int start, int end){
        assert buffer != null;
        if (end - start <= 1)
            return;

        int m = partition(buffer, start, end);
        quickSort(buffer, start, m);
        quickSort(buffer, m+1, end);
    }

    private static int partition(IntBuffer buffer, int start, int end){
        int il = start;
        int ir = end - 1;
        int pivot = buffer.get(ir);
        int temp;
        while (ir > il){
            if (buffer.get(ir - 1) > pivot){
                buffer.put(ir, buffer.get(ir - 1));
                ir--;
            } else {
                temp = buffer.get(ir - 1);
                buffer.put(ir - 1, buffer.get(il));
                buffer.put(il, temp);
                il++;
            }
        }
        buffer.put(il,pivot);
        return il;
    }


}
