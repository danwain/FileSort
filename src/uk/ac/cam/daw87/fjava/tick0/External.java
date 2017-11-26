package uk.ac.cam.daw87.fjava.tick0;

import com.danwainwright.java.heap.tuple.IntPairBinaryHeap;
import com.danwainwright.java.heap.tuple.IntPairMinHeap;
import uk.ac.cam.daw87.fjava.tick0.helpers.*;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public final class External {
    private final static int INITIAL_SORT_SIZE = 756490;
    private final static int IN_MEMORY_RADIX = 1000000;
    private final static int MAX_HEAP_SIZE = 1000000;
    private final static int WRITER_SIZE = 300000;
    private final static OpenOption[] openOptions = new OpenOption[] {StandardOpenOption.READ, StandardOpenOption.WRITE};

    public static void sort(Path p1, Path p2) throws IOException, IllegalArgumentException, InterruptedException, ExecutionException {
        FileChannel f1 = FileChannel.open(p1, openOptions);
        FileChannel f2 = FileChannel.open(p2, openOptions);
        AsynchronousFileChannel f1a = AsynchronousFileChannel.open(p1, StandardOpenOption.WRITE);
        AsynchronousFileChannel f2a = AsynchronousFileChannel.open(p2, StandardOpenOption.READ);

        final int fileSize = (int) f1.size();
        final int totalInts = fileSize / 4;

        if (fileSize > 4) {
            if (fileSize <= IN_MEMORY_RADIX) {
                sortInMemory(f1, fileSize);
            } else {
                initialSort(f1, f2);
                int groups = Helper.roundUp(totalInts, INITIAL_SORT_SIZE);
                merge(f1a, f2a, groups, fileSize);
            }
        }
    }

    private static void sortInMemory(FileChannel f, int fileSize) throws IOException{
        final ByteBuffer buffer = ByteBuffer.allocate(fileSize);
        f.position(0);
        f.read(buffer);
        buffer.position(0);
        Sorters.RadixSort(buffer.array(), 0, fileSize);
        buffer.position(0);
        f.position(0);
        f.write(buffer);
    }

    private static void merge(AsynchronousFileChannel f1, AsynchronousFileChannel f2, int groups, int fileSize) throws IOException, InterruptedException, ExecutionException{
        assert groups >= 1;
        final IntPairMinHeap heap = new IntPairBinaryHeap(groups);
        ByteBuffer write = ByteBuffer.allocate(WRITER_SIZE);
        ByteBuffer writeWaiting = ByteBuffer.allocate(WRITER_SIZE);
        ByteBuffer tempByteBuffer;
        Future<Integer> futureWrite = null;
        final int amount_in_each = MAX_HEAP_SIZE / groups;
        assert amount_in_each > 0;
        final int[][] lookup = new int[groups][2];
        final ByteBuffer[] readCache = new ByteBuffer[groups];
        final ByteBuffer[] readCacheWaiting = new ByteBuffer[groups];
        final Future<Integer>[] readDone = new Future[groups];
        int writeIndex = 0;

        for (int i = 0; i < lookup.length - 1; i++) {
            lookup[i][0] = (i + 1) * INITIAL_SORT_SIZE * 4;
            lookup[i][1] = i * INITIAL_SORT_SIZE * 4;
        }
        lookup[lookup.length - 1][0] = fileSize;
        lookup[lookup.length - 1][1] = lookup[lookup.length - 2][0];
        Arrays.parallelSetAll(readCache, i -> ByteBuffer.allocate(amount_in_each * 4));
        readCache[readCache.length - 1] = ByteBuffer.allocate((Math.min(fileSize - lookup[lookup.length - 2][1], amount_in_each)));
        Arrays.parallelSetAll(readCacheWaiting, i -> initialRead(f2, lookup, i, readCache[i].limit(), readDone));

        for (int k = 0; k < lookup.length; k++) {
            readBuffer(f2, lookup, k, amount_in_each, readCache, readCacheWaiting, readDone);
            heap.addNoHeaify(readCache[k].getInt(), k);
        }
        heap.build();


        while (!heap.isEmpty()) {
            if (write.position() + 4 > WRITER_SIZE){
                write.flip();
                if (futureWrite != null) {
                    futureWrite.get();
                }
                tempByteBuffer = write;
                write = writeWaiting;
                writeWaiting = tempByteBuffer;

                futureWrite = f1.write(writeWaiting, writeIndex);
                writeIndex += WRITER_SIZE;
                write.clear();
            }
            int[] returnTemp = heap.peek();
            if (readCache[returnTemp[1]].remaining() >= 4 || readBuffer(f2, lookup, returnTemp[1], amount_in_each, readCache, readCacheWaiting, readDone)) {
                assert readCache[returnTemp[1]].remaining() >= 4;
                heap.replaceMin(readCache[returnTemp[1]].getInt(), returnTemp[1]);
            } else {
                heap.popMin();
            }
            write.putInt(returnTemp[0]);
        }
        if (futureWrite != null)
            futureWrite.get();
        write.flip();
        f1.write(write, writeIndex);
    }


    private static boolean readBuffer(AsynchronousFileChannel file, int[][] positions, int i, int toRead, ByteBuffer[] readCache, ByteBuffer[] readCacheWaiting, Future<Integer>[] readFutures) throws IOException, InterruptedException, ExecutionException{
        final int end = positions[i][0];
        int index = positions[i][1];
        final int length = Math.min(Math.min(toRead << 2, end - index), readCache[i].capacity());
        assert (length / 4) * 4 == length;
        assert length >= 0;
        assert readFutures[i] != null;
        readFutures[i].get();
        assert readFutures[i].isDone();
        ByteBuffer temp = readCacheWaiting[i];
        readCacheWaiting[i] = readCache[i];
        readCache[i] = temp;
        readCache[i].position(0);
        if (index == end) {
            assert length == 0;
            readCacheWaiting[i].limit(0);
        } else {
            readCacheWaiting[i].clear();
            readCacheWaiting[i].limit(length);
            readFutures[i] = file.read(readCacheWaiting[i], index);
            index += length;
        }

        positions[i][1] = index;
        assert index <= end;
        assert (readCache[i].limit() / 4) * 4 == readCache[i].limit();
        return readCache[i].limit() != 0;
    }

    private static ByteBuffer initialRead(AsynchronousFileChannel file, int[][] positions, int i, int toRead, Future<Integer>[] future) {
        final int end = positions[i][0];
        int index = positions[i][1];
        ByteBuffer readWaiting = ByteBuffer.allocate(toRead);
        final int length = Math.min(Math.min(toRead << 2, end - index), readWaiting.capacity());
        readWaiting.clear();
        readWaiting.limit(length);
        future[i] = file.read(readWaiting, index);
        positions[i][1] += length;
        return readWaiting;
    }

    private  static void initialSort(FileChannel f1, FileChannel f2) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(INITIAL_SORT_SIZE << 2);
        byte[] array = buffer.array();
        int length;
        while (true) {
            buffer.clear();
            length = f1.read(buffer);
            if (length <= 0)
                break;
            buffer.position(0);
            Sorters.RadixSort(array, 0, length);
            buffer.position(0);
            buffer.limit(length);
            f2.write(buffer);
        }
    }
}
