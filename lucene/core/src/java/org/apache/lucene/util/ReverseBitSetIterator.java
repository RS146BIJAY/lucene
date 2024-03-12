package org.apache.lucene.util;

import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;

public class ReverseBitSetIterator extends DocIdSetIterator {

    private final BitSet bits;
    private final int length;
    private final long cost;
    private int doc;

    /** Sole constructor. */
    public ReverseBitSetIterator(BitSet bits, long cost) {
        if (cost < 0) {
            throw new IllegalArgumentException("cost must be >= 0, got " + cost);
        }
        this.bits = bits;
        this.length = bits.length();
        this.cost = cost;
        this.doc = length;
    }

    @Override
    public int docID() {
        return doc;
    }

    /** Set the current doc id that this iterator is on. */
    public void setDocId(int docId) {
        this.doc = docId;
    }

    @Override
    public int nextDoc() throws IOException {
        return advance(doc - 1);
    }

    @Override
    public int advance(int target) {
        if (target < 0) {
            return doc = -1;
        }
        return doc = bits.prevSetBit(target);
    }

    @Override
    public long cost() {
        return cost;
    }

    @Override
    public DocIdSetIterator reverseIterator() {
        return new BitSetIterator(bits, cost);
    }
}
