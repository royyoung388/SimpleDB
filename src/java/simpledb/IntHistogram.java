package simpledb;

import java.util.Arrays;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int[] count;
    private int width, start, total;

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        width = (int) Math.ceil((max - min + 1) / (double) buckets);
        start = min;
        count = new int[buckets];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        int index = (int) Math.floor((v - start) / (double) width);
        count[index]++;
        total++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int index = (int) Math.floor((v - start) / (float) width);
        int lower = index * width + start;
        int right, left;
        switch (op) {
            case EQUALS:
                if (index < 0 || index >= count.length)
                    return 0;
                return count[index] / (double) width / total;
            case NOT_EQUALS:
                if (index < 0 || index >= count.length)
                    return 1;
                return 1 - count[index] / (double) width / total;
            case GREATER_THAN:
                if (index < 0)
                    return 1;
                if (index >= count.length)
                    return 0;
                right = 0;
                for (int i = index + 1; i < count.length; i++) {
                    right += count[i];
                }
                return ((double) (width - (v - lower) - 1) * count[index] / width + right) / total;
            case GREATER_THAN_OR_EQ:
                if (index < 0)
                    return 1;
                if (index >= count.length)
                    return 0;
                right = 0;
                for (int i = index + 1; i < count.length; i++) {
                    right += count[i];
                }
                return ((double) (width - (v - lower)) * count[index] / width + right) / total;
            case LESS_THAN:
                if (index < 0)
                    return 0;
                if (index >= count.length)
                    return 1;
                left = 0;
                for (int i = 0; i < index; i++) {
                    left += count[i];
                }
                return ((double) (v - lower) * count[index] / width + left) / total;
            case LESS_THAN_OR_EQ:
                if (index < 0)
                    return 0;
                if (index >= count.length)
                    return 1;
                left = 0;
                for (int i = 0; i < index; i++) {
                    left += count[i];
                }
                return ((double) (v - lower + 1) * count[index] / width + left) / total;
        }
        // some code goes here
        return -1.0;
    }

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return Arrays.toString(count);
    }
}
