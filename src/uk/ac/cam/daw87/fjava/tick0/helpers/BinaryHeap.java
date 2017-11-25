package uk.ac.cam.daw87.fjava.tick0.helpers;


public class BinaryHeap {
    public static void insert(int[][] Heap, int HeapSize, int number, int filePosition, int[] temp){
        addNoHeaify(Heap, HeapSize, number, filePosition);
        heapifyUP(Heap, HeapSize + 1, temp);
    }

    public static void addNoHeaify(int[][] Heap, int HeapSize, int number, int filePosition){
        assert HeapSize <= Heap.length;
        Heap[HeapSize][0] = number;
        Heap[HeapSize][1] = filePosition;
        //HeapSize++;
    }

    public static void build(int[][] Heap, int HeapSize, int[] temp){
        for (int i = (HeapSize + 1) / 2; i >= 0 ; i--) {
            heapifyDown(Heap, HeapSize, i, temp);
        }
    }

    private static void heapifyUP(int[][] Heap, int HeapSize, int[] temp){
        int index = HeapSize - 1;

        while (hasParent(index) && parent(Heap, index)[0] > Heap[index][0]){
            swap(Heap, index, parentIndex(index), temp);
            index = parentIndex(index);
        }
    }

    public static void heapifyDown(int[][] Heap, int HeapSize, int start, int[] temp){
        int index = start;

        while (hasLeftChild(index, HeapSize)){
            int smallest = leftIndex(index);
            assert smallest > 0;
            if (hasRightChild(index, HeapSize) && Heap[smallest][0] > Heap[rightIndex(index)][0]){
                smallest = rightIndex(index);
            }

            if (Heap[index][0] > Heap[smallest][0]){
                swap(Heap, index, smallest, temp);
            } else {
                break;
            }

            index = smallest;
        }
    }

    public static boolean isEmpty(int HeapSize){
        return HeapSize == 0;
    }

    private static boolean hasParent(int i) {
        return i > 0;
    }


    private static int leftIndex(int i) {
        return i * 2 + 1;
    }


    private static int rightIndex(int i) {
        return i * 2 + 2;
    }


    private static boolean hasLeftChild(int i, int HeapSize) {
        return leftIndex(i) < HeapSize;
    }

    private static boolean hasRightChild(int i, int HeapSize) {
        return rightIndex(i) < HeapSize;
    }

    private static int parentIndex(int i) {
        return (i-1) / 2;
    }

    private static int[] parent(int[][] Heap, int i) {
        return Heap[parentIndex(i)];
    }

    private static void swap(int[][] Heap, int i1, int i2, int[] temp){
        temp[0] = Heap[i1][0];
        temp[1] = Heap[i1][1];


        Heap[i1][0] = Heap[i2][0];
        Heap[i1][1] = Heap[i2][1];

        Heap[i2][0] = temp[0];
        Heap[i2][1] = temp[1];
    }


    public static void getMin(int[][] Heap, int HeapSize, int[] result, int[] temp){
        assert !isEmpty(HeapSize);
        result[0] = Heap[0][0];
        result[1] = Heap[0][1];
        swap( Heap, 0, HeapSize - 1, temp);
        heapifyDown(Heap, HeapSize - 1, 0, temp);
    }

    public static void peek(int[][] Heap, int HeapSize, int[] result){
        assert !isEmpty(HeapSize);
        result[0] = Heap[0][0];
        result[1] = Heap[0][1];
    }
}
