package uk.ac.cam.daw87.fjava.tick0.helpers;

public class BinaryHeap {
    private int[][] Heap;
    private int[] temp;
    private int HeapSize;
    private boolean Clean;

    public BinaryHeap(int size){
        HeapSize = 0;
        Heap = new int[size][2];
        Clean = true;
        temp = new int[2];
    }

    public void insert(int number, int filePosition){
        Heap[HeapSize][0] = number;
        Heap[HeapSize][1] = filePosition;
        HeapSize++;
        Clean = false;
    }

    public void heapifyUP(){
        int index = HeapSize;

        while (hasParent(index) && parent(index)[0] > Heap[index][0]){
            swap(index, parentIndex(index));
            index = parentIndex(index);
        }

        Clean = true;
    }

    private void heapifyDown(){
        int index = 1;

        while (hasLeftChild(index)){
            int smallest = leftIndex(index);

            if (hasRightChild(index) && Heap[smallest][0] > Heap[rightIndex(index)][0]){
                smallest = rightIndex(index);
            }

            if (Heap[index][0] > Heap[smallest][0]){
                swap(index, smallest);
            } else {
                break;
            }

            index = smallest;
        }
    }

    public boolean isEmpty(){
        return HeapSize == 0;
    }

    private static boolean hasParent(int i) {
        return i > 1;
    }


    private static int leftIndex(int i) {
        return i * 2;
    }


    private static int rightIndex(int i) {
        return i * 2 + 1;
    }


    private boolean hasLeftChild(int i) {
        return leftIndex(i) <= HeapSize;
    }


    private boolean hasRightChild(int i) {
        return rightIndex(i) <= HeapSize;
    }

    private static int parentIndex(int i) {
        return i / 2;
    }

    private int[] parent(int i) {
        return Heap[parentIndex(i)];
    }

    private void swap(int i1, int i2){
        temp[0] = Heap[i1][0];
        temp[1] = Heap[i1][1];


        Heap[i1][0] = Heap[i2][0];
        Heap[i1][1] = Heap[i2][1];

        Heap[i2][0] = temp[0];
        Heap[i2][1] = temp[1];
    }


    public int[] getMin(){
        assert Clean;
        int[] min = Heap[0];
        HeapSize--;
        heapifyDown();
        return min;
    }

}
