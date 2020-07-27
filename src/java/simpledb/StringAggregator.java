package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class StringAggregator extends AggregatorImpl {

    public StringAggregator(int gfield, Type gfieldtype, int afield, Op what) {
        super(gfield, gfieldtype, afield, what);
    }

    @Override
    protected IntField aggregate(List<Field> group, Op op) {
        switch (op) {
            case COUNT:
                return new IntField(group.size());
            default:
                throw new RuntimeException("Op not supported: " + op.name());
        }
    }
}
