package com.danwainwright.java.heap.tuple;

import java.util.NoSuchElementException;

/**
 * This is a minimum heap that takes a pair of int values.
 * The minimum is found using just the key value but when returning the other value is returned.
 */
public interface IntPairMinHeap {
    /**
     * Add an item to the heap and maintain the heap is valid.
     * @param key The key to be sorted on
     * @param feature The extra tag that is kept with the key
     * @throws IllegalStateException if there are capacity issues then this is thrown
     */
    void insert(int key, int feature) throws IllegalStateException;

    /**
     * Add an item to the heap but don't maintain it is still valid.
     * See {@link #build()} to make valid again.
     * @param key The key to be sorted on
     * @param feature The extra tag that is kept with the key
     * @throws IllegalStateException if there are capacity issues then this is thrown
     */
    void addNoHeapify(int key, int feature) throws IllegalStateException;

    /**
     * Peek at the minimum item in the heap.
     * @return an int array of size 2, where key is index 0, and feature index 1.
     * @throws NoSuchElementException is the heap is empty
     */
    int[] peek() throws NoSuchElementException;

    /**
     * Removes the min item from the heap and leaves the heap in a valid state.
     * @return an int array of size 2, where key is index 0, and feature index 1.
     * @throws NoSuchElementException is the heap is empty
     */
    int[] popMin() throws NoSuchElementException;

    /**
     * Checks if the heap has any items in it.
     * @return true if empty
     */
    boolean isEmpty();

    /**
     * After running it maintains that the heap is valid.
     * This is useful when adding items with {@link #addNoHeapify(int, int)}.
     */
    void build();

    /**
     * Replace the min item and ensure heap is valid. Useful when there is a max size of heap.
     * @param key The key to be sorted on
     * @param feature The extra tag that is kept with the key
     */
    void replaceMin(int key, int feature);
}
