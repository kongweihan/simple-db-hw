package simpledb;

import com.sun.source.doctree.HiddenTree;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.ioCostPerPage = ioCostPerPage;

        try {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(tableid);
            numPages = dbFile.numPages();

            TupleDesc td = dbFile.getTupleDesc();
            histograms = new Histogram[td.numFields()];

            // Scan and get min/max for each IntField, and numTuples
            int[] min = new int[td.numFields()];
            int[] max = new int[td.numFields()];
            for (int i = 0; i < min.length; i++) {
                min[i] = Integer.MAX_VALUE;
                max[i] = Integer.MIN_VALUE;
            }
            List<Integer> intFields = getIntFieldIndexes(td);
            int tupleCount = 0;
            DbFileIterator it = dbFile.iterator(null);
            it.open();
            while (it.hasNext()) {
                Tuple tuple = it.next();
                for (int i : intFields) {
                    int value = ((IntField) tuple.getField(i)).getValue();
                    min[i] = Math.min(min[i], value);
                    max[i] = Math.max(max[i], value);
                }
                tupleCount++;
            }
            numTuples = tupleCount;

            // Set up histograms
            for (int i = 0; i < td.numFields(); i++) {
                if (td.getFieldType(i) == Type.INT_TYPE) {
                    histograms[i] = new IntHistogram(NUM_HIST_BINS, min[i], max[i]);
                } else if (td.getFieldType(i) == Type.STRING_TYPE) {
                    histograms[i] = new StringHistogram(NUM_HIST_BINS);
                } else {
                    throw new DbException("Unsupported Type for table statistics:" + td.getFieldType(i));
                }
            }

            // Scan and compute histograms
            it = dbFile.iterator(null);
            it.open();
            while (it.hasNext()) {
                Tuple tuple = it.next();
                for (int i = 0; i < td.numFields(); i++) {
                    if (td.getFieldType(i) == Type.INT_TYPE) {
                        IntHistogram hist = (IntHistogram) histograms[i];
                        hist.addValue(((IntField) tuple.getField(i)).getValue());
                    } else if (td.getFieldType(i) == Type.STRING_TYPE) {
                        StringHistogram hist = (StringHistogram) histograms[i];
                        hist.addValue(tuple.getField(i).toString());
                    } else {
                        throw new DbException("Unsupported Type for table statistics:" + td.getFieldType(i));
                    }
                }
            }
        } catch (DbException | TransactionAbortedException e) {
            throw new RuntimeException(e);
        }
    }

    private final int numTuples, numPages;
    private final Histogram[] histograms;
    private final int ioCostPerPage;

    private List<Integer> getIntFieldIndexes(TupleDesc td) {
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < td.numFields(); i++) {
            if (td.getFieldType(i) == Type.INT_TYPE) {
                list.add(i);
            }
        }
        return list;
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return ioCostPerPage * numPages;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (selectivityFactor * numTuples);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return histograms[field].avgSelectivity(op);
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        if (constant.getType() == Type.INT_TYPE) {
            IntHistogram hist = (IntHistogram) histograms[field];
            return hist.estimateSelectivity(op, ((IntField) constant).getValue());
        } else if (constant.getType() == Type.STRING_TYPE) {
            StringHistogram hist = (StringHistogram) histograms[field];
            return hist.estimateSelectivity(op, constant.toString());
        } else {
            throw new RuntimeException("estimateSelectivity() unknown Type: " + constant.getType());
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return numTuples;
    }

    public Histogram getHistogram(int field) {
        return histograms[field];
    }

}
