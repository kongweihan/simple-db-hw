package simpledb;

import java.util.HashMap;
import java.util.Map;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram implements Histogram<Integer> {

    private final int[] buckets;
    private final int min, max;
    private final double width;
    private int total;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = new int[buckets];
        this.min = min;
        this.max = max;
        this.width = (max - min) / (double) buckets;
        // No need to precisely define what are the lower and upper bound
        // of the buckets' range.
        // Only need to deterministically determine which bucket a given value falls into
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    @Override
    public void addValue(Integer v) {
    	// some code goes here
        buckets[getBucket(v)]++;
        total++;
    }

    // For test compatibility
    public void addValue(int v) {
        addValue((Integer) v);
    }

    /** The bucket index of a given value is:
     * (v - min) / width
     * except that if the computed index >= buckets, assign it to buckets-1
     * The bucket ranges are essentially [lowerbound, lowerbound+width), open interval,
     * except that the last bucket is closed interval
     * @param v Any value within the range of min and max
     */
    private int getBucket(int v) {
        if (v < min || v > max) {
            throw new RuntimeException(String.format("IntHistogram value %d out of bound: [%d,%d]", v, min, max));
        }
        int idx = (int)((v - min) / width);
        idx = idx >= buckets.length ? buckets.length - 1 : idx;
        return idx;
    }

    private double getLowerbound(int b) {
        return min + b * width;
    }

    private double getUpperbound(int b) {
        if (b == buckets.length - 1) {
            return max;
        }
        return min + (b + 1) * width;
    }

    // The mininum integer value that belongs to bucket b
    private int getBucketMin(int b) {
        return (int) Math.ceil(getLowerbound(b));
    }

    // The maxinum integer value that belongs to bucket b
    private int getBucketMax(int b) {
        if (b == buckets.length - 1) {
            return max;
        }
        double floor = Math.floor(getUpperbound(b));
        if (floor == getUpperbound(b)) {
            return (int) floor - 1;
        } else {
            return (int) floor;
        }
    }

    private int numDistinctValues(int b) {
        return getBucketMax(b) - getBucketMin(b) + 1;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    @Override
    public double estimateSelectivity(Predicate.Op op, Integer v) {
    	// some code goes here
        switch (op) {
            case EQUALS:
                return selectivityEquals(v);
            case GREATER_THAN:
                return selectivityGreater(v);
            case GREATER_THAN_OR_EQ:
                return selectivityEquals(v) + selectivityGreater(v);
            case LESS_THAN:
                return selectivityLess(v);
            case LESS_THAN_OR_EQ:
                return selectivityEquals(v) + selectivityLess(v);
            case NOT_EQUALS:
                return 1 - selectivityEquals(v);
            default:
                throw new RuntimeException("estimateSelectivity op not supported:" + op);
        }
    }

    public double estimateSelectivity(Predicate.Op op, int v) {
        return estimateSelectivity(op, (Integer) v);
    }

    private double selectivityEquals(int v) {
        if (v < min || v > max) {
            return 0;
        }
        int b = getBucket(v);
        return (double) buckets[b] / numDistinctValues(b) / total;
    }

    private double selectivityGreater(int v) {
        if (v < min) {
            return 1.0;
        }
        if (v >= max) {
            return 0;
        }
        int count = 0;
        for (int b = getBucket(v) + 1; b < buckets.length; b++) {
            count += buckets[b];
        }
        return selecvivityGreaterOfBucket(v) + (double) count / total;
    }

    private double selecvivityGreaterOfBucket(int v) {
        int b = getBucket(v);
        return (double) buckets[b] / numDistinctValues(b) * (getBucketMax(b) - v);
    }

    private double selectivityLess(int v) {
        if (v <= min) {
            return 0;
        }
        if (v > max) {
            return 1.0;
        }
        int count = 0;
        for (int b = getBucket(v) - 1; b >= 0; b--) {
            count += buckets[b];
        }
        return selecvivityLessOfBucket(v) + (double) count / total;
    }

    private double selecvivityLessOfBucket(int v) {
        int b = getBucket(v);
        return (double) buckets[b] / numDistinctValues(b) * (v - getBucketMin(b)) / total;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    @Override
    public double avgSelectivity()
    {
        // some code goes here
        return avgSelectivity(Predicate.Op.EQUALS);
    }

    @Override
    public double avgSelectivity(Predicate.Op op) {
        double sum = 0;
        for (int v = min; v <= max; v++) {
            switch (op) {
                case EQUALS:
                    sum += selectivityEquals(v);
                    break;
                case GREATER_THAN:
                    sum += selectivityGreater(v);
                    break;
                case GREATER_THAN_OR_EQ:
                    sum += selectivityEquals(v) + selectivityGreater(v);
                    break;
                case LESS_THAN:
                    sum += selectivityLess(v);
                    break;
                case LESS_THAN_OR_EQ:
                    sum += selectivityEquals(v) + selectivityLess(v);
                    break;
                case NOT_EQUALS:
                    sum += 1 - selectivityEquals(v);
                    break;
                default:
                    throw new RuntimeException("estimateSelectivity op not supported:" + op);
            }
        }
        return sum / buckets.length;
    }

    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        sb.append("IntHistogram total:" + total + " buckets: ");
        for (int b = 0; b < buckets.length - 1; b++) {
            sb.append(getBucketMin(b) + ":" + buckets[b] + ",");
        }
        sb.append(getBucketMin(buckets.length - 1) + ":" + buckets[buckets.length - 1]);
        return sb.toString();
    }
}
