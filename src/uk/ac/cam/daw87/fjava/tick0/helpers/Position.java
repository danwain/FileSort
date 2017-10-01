package uk.ac.cam.daw87.fjava.tick0.helpers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public final class Position{
    public final int Initial_Size;
    public final int Start_Position;
    private int position;
    public final FileChannel file;
    public int amountInHeap;

    public Position(int start, int size, FileChannel file){
        Initial_Size = size;
        Start_Position = start;
        position = Start_Position;
        this.file = file;
        amountInHeap = 0;
    }

    public final int getNext(int amount, ByteBuffer buffer) throws IOException{
        assert (position - Start_Position) <= Initial_Size;
        if (position - Start_Position == Initial_Size)
            return 0;
        buffer.position(0);
        file.position(position * 4);
        int length = Math.min(amount, Start_Position + Initial_Size - position);
        buffer.limit(length * 4);
        int read = file.read(buffer);
        assert (read >= length * 4);
        position+=length;
        return length;
    }

    @Override
    public String toString() {
        return "Position{" +
                "Initial_Size=" + Initial_Size +
                ", Start_Position=" + Start_Position +
                ", position=" + position +
                '}';
    }
}
