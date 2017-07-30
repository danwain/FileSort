package uk.ac.cam.daw87.oop.summer.Sort.helpers;

public final class Number implements Comparable<Number> {
    public final int number;
    public final Position group;

    public Number(int n, Position g){
        number = n;
        group = g;
    }

    @Override
    public int compareTo(Number number) {
        return Integer.compare(this.number, number.number);
    }
}
