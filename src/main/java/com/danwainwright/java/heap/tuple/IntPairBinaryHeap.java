package com.danwainwright.java.heap.tuple;

import java.util.Arrays;
import java.util.NoSuchElementException;

public class IntPairBinaryHeap implements IntPairMinHeap{

    private int[][] Heap;
    private int[] temp;
    private int[] returnTemp;
    private int HeapSize;

    /**
     *
     * @param size This is the maximum size of the heap.
     */
    public IntPairBinaryHeap(int size) throws IllegalArgumentException{
        if (size < 0)
            throw new IllegalArgumentException(String.format("Size must not be less than 0, actual value was %d", size));
        HeapSize = 0;
        Heap = new int[size][2];
        temp = new int[2];
        returnTemp = new int[2];
    }

    @Override
    public void insert(int number, int filePosition) throws IllegalStateException {
        if (HeapSize >= Heap.length)
            throw new IllegalStateException();
        addNoHeapify(number, filePosition);
        heapifyUP();
    }

    @Override
    public void replaceMin(int number, int filePosition){
        Heap[0][0]= number;
        Heap[0][1] = filePosition;
        heapifyDown(0);
    }

    @Override
    public void addNoHeapify(int number, int filePosition) throws IllegalStateException{
        if (HeapSize >= Heap.length)
            throw new IllegalStateException();
        Heap[HeapSize][0] = number;
        Heap[HeapSize][1] = filePosition;
        HeapSize++;
    }

    @Override
    public void build(){
        for (int i = HeapSize / 2; i > 0 ; i--)
            heapifyDown(i);
    }

    private void heapifyUP(){
        int index = HeapSize - 1;

        while (hasParent(index) && parent(index)[0] > Heap[index][0]){
            swap(index, parentIndex(index));
            index = parentIndex(index);
        }
    }

    private void heapifyDown(int start){
        int index = start;

        while (hasLeftChild(index)){
            int smallest = leftIndex(index);
            assert smallest > 0;
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

    @Override
    public boolean isEmpty(){
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


    private boolean hasLeftChild(int i) {
        return leftIndex(i) < HeapSize;
    }

    private boolean hasRightChild(int i) {
        return rightIndex(i) < HeapSize;
    }

    private static int parentIndex(int i) {
        return (i-1) / 2;
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


    @Override
    public int[] popMin() throws NoSuchElementException{
        if (isEmpty())
            throw new NoSuchElementException();
        peek();
        swap( 0, HeapSize - 1);
        HeapSize--;
        heapifyDown(0);
        return returnTemp;
    }

    @Override
    public int[] peek() throws NoSuchElementException{
        if (isEmpty())
            throw new NoSuchElementException();
        returnTemp[0] = Heap[0][0];
        returnTemp[1] = Heap[0][1];
        return returnTemp;
    }


    @Override
    public String toString() {
        int[] tempArray = new int[HeapSize];
        for (int i = 0; i < HeapSize; i++) {
            tempArray[i] = Heap[i][0];
        }
        return Arrays.toString(tempArray);
    }
}
