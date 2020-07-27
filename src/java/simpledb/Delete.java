package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private TupleDesc td;
    private TransactionId tid;
    private List<Tuple> deleted;  // single-element list
    private Iterator<Tuple> it;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.child = child;
        this.tid = t;
        this.td = new TupleDesc(new Type[]{ Type.INT_TYPE });
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        try {
            child.open();
            int count = 0;
            while (child.hasNext()) {
                Database.getBufferPool().deleteTuple(tid, child.next());
                count++;
            }
            child.close();
            deleted = new ArrayList<>(Arrays.asList(new Tuple(td)));
            deleted.get(0).setField(0, new IntField(count));
            super.open();
            it = deleted.iterator();
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
        it = deleted.iterator();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
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
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}
