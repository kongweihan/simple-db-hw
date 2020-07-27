package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator extends AggregatorImpl {

    public IntegerAggregator(int gfield, Type gfieldtype, int afield, Op what) {
        super(gfield, gfieldtype, afield, what);
    }

    @Override
    protected IntField aggregate(List<Field> group, Op op) {
        switch (op) {
            case COUNT:
                return new IntField(group.size());
            case SUM:
                int sum = 0;
                for (Field field : group) {
                    sum += ((IntField) field).getValue();
                }
                return new IntField(sum);
            case AVG:
                sum = 0;
                for (Field field : group) {
                    sum += ((IntField) field).getValue();
                }
                return new IntField(sum / group.size());
            case MIN:
                int min = Integer.MAX_VALUE;
                for (Field field : group) {
                    min = Math.min(min, ((IntField) field).getValue());
                }
                return new IntField(min);
            case MAX:
                int max = Integer.MIN_VALUE;
                for (Field field : group) {
                    max = Math.max(max, ((IntField) field).getValue());
                }
                return new IntField(max);
            default:
                throw new RuntimeException("Op not supported: " + op.name());
        }
    }
}
