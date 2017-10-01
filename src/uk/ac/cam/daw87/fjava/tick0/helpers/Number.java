package uk.ac.cam.daw87.fjava.tick0.helpers;


public final class Number implements Comparable<Number> {
    public final int number;
    public final Position group;

    public Number(int n, Position g){
        number = n;
        group = g;
    }

    @Override
    public int compareTo(Number number) {
        return this.number > number.number ? 1 : this.number < number.number ? -1 : 0;
    }
}
