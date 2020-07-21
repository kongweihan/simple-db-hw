package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        @Override
        public boolean equals(Object o) {
            // some code goes here
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TDItem item = (TDItem) o;
            return this.fieldType == item.fieldType && this.fieldName == item.fieldName;
        }

        @Override
        public int hashCode() {
            return this.fieldType.hashCode() + this.fieldName.hashCode();
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private TDItem[] items;
    private int size;  // in bytes

    public static class TupleDescIterator implements Iterator<TDItem> {

        private final TupleDesc td;
        private int it = 0;

        public TupleDescIterator(TupleDesc td) {
            this.td = td;
        }

        @Override
        public boolean hasNext() {
            return it < td.items.length;
        }

        @Override
        public TDItem next() {
            return td.items[it++];
        }
    }


    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return new TupleDescIterator(this);
    }

    private static final long serialVersionUID = 1L;

    private static TDItem[] initItems(Type[] typeAr, String[] fieldAr) {
        if (typeAr.length == 0) {
            throw new IllegalArgumentException("typeAr must have non-zero length.");
        }
        TDItem[] items = new TDItem[typeAr.length];
        for (int i = 0; i < typeAr.length; i++) {
            if (fieldAr == null) {
                items[i] = new TDItem(typeAr[i], null);
            } else {
                items[i] = new TDItem(typeAr[i], fieldAr[i]);
            }
        }
        return items;
    }

    private static int computeSize(TDItem[] items) {
        int size = 0;
        for (int i = 0; i < items.length; i++) {
            size += items[i].fieldType.getLen();
        }
        return size;
    }

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        if (fieldAr != null && typeAr.length != fieldAr.length) {
            throw new IllegalArgumentException("typeAr and fieldAr must have same length.");
        }
        items = initItems(typeAr, fieldAr);
        size = computeSize(items);
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        items = initItems(typeAr, null);
        size = computeSize(items);
    }

    public TupleDesc(TDItem[] items) {
        this.items = items;
        size = computeSize(items);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return items.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= items.length) {
            throw new IllegalArgumentException("Field name index out of range, max: " + (items.length - 1));
        }
        return items[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= items.length) {
            throw new IllegalArgumentException("Field type index out of range, max: " + (items.length - 1));
        }
        return items[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        if (name == null) {
            throw new NoSuchElementException("null is not a valid field name.");
        }
        // some code goes here
        for (int i = 0; i < items.length; i++) {
            if (items[i].fieldName != null && items[i].fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException("Field name '" + name + "' is not found.");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        TDItem[] items = new TDItem[td1.numFields() + td2.numFields()];
        int i = 0;
        Iterator<TDItem> it = td1.iterator();
        while (it.hasNext()) {
            items[i] = it.next();
            i++;
        }
        it = td2.iterator();
        while (it.hasNext()) {
            items[i] = it.next();
            i++;
        }
        return new TupleDesc(items);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    @Override
    public boolean equals(Object o) {
        // some code goes here
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TupleDesc td = (TupleDesc) o;
        if (this.numFields() != td.numFields() || this.getSize() != td.getSize()) {
            return false;
        }
        for (int i = 0; i < items.length; i++) {
            if (!items[i].equals(td.items[i])) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
//        throw new UnsupportedOperationException("unimplemented");
        return toString().hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < items.length; i++) {
            sb.append(items[i].fieldType);
            sb.append("(");
            sb.append(items[i].fieldName);
            sb.append(")");
        }
        return sb.toString();
    }
}
