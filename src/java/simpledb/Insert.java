package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private TupleDesc td;
    private TransactionId tid;
    private int tableId;
    private List<Tuple> inserted;  // single-element list
    private Iterator<Tuple> it;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.child = child;
        this.tableId = tableId;
        this.tid = t;
        this.td = new TupleDesc(new Type[]{ Type.INT_TYPE });
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        try {
            child.open();
            int count = 0;
            while (child.hasNext()) {
                Database.getBufferPool().insertTuple(tid, tableId, child.next());
                count++;
            }
            child.close();
            inserted = new ArrayList<>(Arrays.asList(new Tuple(td)));
            inserted.get(0).setField(0, new IntField(count));
            super.open();
            it = inserted.iterator();
        } catch (IOException e) {
            throw new DbException(e.toString());
        }
    }

    public void close() {
        // some code goes here
        super.close();
        it = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        it = inserted.iterator();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (it != null && it.hasNext()) {
            return it.next();
        }
        return null;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] { this.child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}
