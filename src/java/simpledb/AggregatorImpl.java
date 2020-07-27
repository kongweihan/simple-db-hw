package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public abstract class AggregatorImpl implements Aggregator {

    private static final long serialVersionUID = 1L;

    protected final int gfield, afield;
    protected final Type gfieldType;
    protected final Op op;
    protected TupleDesc td;
    private final Map<Field, List<Field>> aggGroups = new HashMap<>();
    private final Map<Field, Tuple> aggResults = new HashMap<>();

    /**
     * Aggregate constructor
     *
     * @param gfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public AggregatorImpl(int gfield, Type gfieldtype, int afield, Op what) {
        // some code goes here
        this.gfield = gfield;
        this.afield = afield;
        this.gfieldType = gfieldtype;
        this.op = what;

        if (gfield == Aggregator.NO_GROUPING) {
            TupleDesc.TDItem item = new TupleDesc.TDItem(
                    Type.INT_TYPE, null);
            td = new TupleDesc(new TupleDesc.TDItem[]{ item });
        } else {
            TupleDesc.TDItem gitem = new TupleDesc.TDItem(
                    gfieldType, null);
            TupleDesc.TDItem aitem = new TupleDesc.TDItem(
                    Type.INT_TYPE, null);
            td = new TupleDesc(new TupleDesc.TDItem[]{ gitem, aitem });
        }
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (gfield == Aggregator.NO_GROUPING) {
            if (!aggGroups.containsKey(null)) {
                aggGroups.put(null, new ArrayList<>());
            }
            aggGroups.get(null).add(tup.getField(afield));
        } else {
            Field key = tup.getField(gfield);
            if (!aggGroups.containsKey(key)) {
                aggGroups.put(key, new ArrayList<>());
            }
            aggGroups.get(key).add(tup.getField(afield));
        }
    }

    protected abstract IntField aggregate(List<Field> group, Op op);

    private Iterator<Field> computeAggregation() {
        Iterator<Field> it = aggGroups.keySet().iterator();
        while (it.hasNext()) {
            Field group = it.next();
            if (group == null) {
                Tuple tuple = new Tuple(td);
                tuple.setField(0, aggregate(aggGroups.get(group), op));
                aggResults.put(group, tuple);
            } else {
                Tuple tuple = new Tuple(td);
                tuple.setField(0, group);
                tuple.setField(1, aggregate(aggGroups.get(group), op));
                aggResults.put(group, tuple);
            }
        }
        return aggResults.keySet().iterator();
    }

    /**
     * Create a OpIterator over group aggregate results.
     * @see simpledb.TupleIterator for a possible helper
     */
    public OpIterator iterator() {
        return new AggregatorIterator(this);
    }

    public static class AggregatorIterator implements OpIterator {
        private final AggregatorImpl aggregator;

        private AggregatorIterator(AggregatorImpl aggregator){
            this.aggregator = aggregator;
        }

        private Iterator<Field> groupIt;

        @Override
        public void open() throws DbException, TransactionAbortedException {
            groupIt = aggregator.computeAggregation();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (groupIt == null) {
                throw new IllegalStateException("Iterator not open.");
            }
            return groupIt.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (groupIt == null) {
                throw new IllegalStateException("Iterator not open.");
            }
            Field group = groupIt.next();
            return aggregator.aggResults.get(group);
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if (groupIt == null) {
                throw new IllegalStateException("Iterator not open.");
            }
            groupIt = aggregator.aggResults.keySet().iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return aggregator.td;
        }

        @Override
        public void close() {
            groupIt = null;
        }
    }
}