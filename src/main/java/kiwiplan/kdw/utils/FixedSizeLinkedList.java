package kiwiplan.kdw.utils;

import java.util.LinkedList;

public class FixedSizeLinkedList<E> extends LinkedList<E> {
    private int fixedSize;

    public FixedSizeLinkedList(int size) {
        this.fixedSize = size;
    }

    public void appendObject(E e) {
        super.add(e);

        if (this.fixedSize > 0) {
            while (this.size() > this.fixedSize) {
                this.remove(0);
            }
        }
    }

    public static void main(String args[]) {
        FixedSizeLinkedList<String> fixedList = new FixedSizeLinkedList(10);

        for (int i = 0; i < 100; i++) {
            fixedList.appendObject(i + "");
            System.out.println(fixedList.toString());
        }
    }
}
