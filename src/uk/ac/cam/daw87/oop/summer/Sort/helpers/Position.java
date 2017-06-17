package uk.ac.cam.daw87.oop.summer.Sort.helpers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public final class Position{
    private final int Initial_Size;
    private final int Start_Position;
    private int position;
    private final FileChannel file;

    public Position(int start, int size, FileChannel file){
        Initial_Size = size;
        Start_Position = start;
        position = Start_Position;
        this.file = file;
    }

    public int getNext(int amount, ByteBuffer buffer) throws IOException{
        assert buffer.capacity() >= amount * 4;
        if (position - Start_Position == Initial_Size)
            return 0;
        file.position(position * 4);
        assert (position - Start_Position) <= Initial_Size;
        int length = Math.min(amount, Start_Position + Initial_Size - position);
        buffer.position(0);
        int read = file.read(buffer);
        assert (read == length * 4);
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
