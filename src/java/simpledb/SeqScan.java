package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    private int tableId;
    private String alias;
    private TupleDesc prefixedTupleDesc;
    private DbFileIterator it;

    private void init(int tableId, String tableAlias) {
        this.tableId = tableId;
        alias = tableAlias;
        prefixedTupleDesc = createPrefixedTupleDesc();
    }

    private TupleDesc createPrefixedTupleDesc() {
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        TupleDesc.TDItem[] items = new TupleDesc.TDItem[file.getTupleDesc().numFields()];
        Iterator<TupleDesc.TDItem> it = file.getTupleDesc().iterator();
        int i = 0;
        while (it.hasNext()) {
            TupleDesc.TDItem item = it.next();
            items[i] = new TupleDesc.TDItem(item.fieldType, String.format("%s.%s", alias, item.fieldName));
            i++;
        }
        return new TupleDesc(items);
    }

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableId
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableId, String tableAlias) {
        // some code goes here
        init(tableId, tableAlias);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here
        return alias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        init(tableid, tableAlias);
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
//        System.out.printf("open table: %d %s\n", tableId, it);
        it = Database.getCatalog().getDatabaseFile(tableId).iterator(null);
        it.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return prefixedTupleDesc;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (it == null) {
            throw new IllegalStateException("SeqScan is not opened.");
        }
        return it.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if (it == null) {
            throw new IllegalStateException("SeqScan is not opened.");
        }
        return it.next();
    }

    public void close() {
        // some code goes here
//        System.out.printf("close SeqScan table: %d\n", tableId);
        if (it != null) {
            it.close();
        }
        it = null;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        if (it == null) {
            throw new IllegalStateException("SeqScan is not opened.");
        }
        it.rewind();
    }
}
