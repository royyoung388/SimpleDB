package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int afield, gbfield;
    private Type gbfieldtype;
    private Op op;
    private HashMap<Field, Integer[]> map;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        op = what;
        map = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field key = null;
        if (gbfield != NO_GROUPING)
            key = tup.getField(gbfield);

        Integer[] values = map.get(key);
        int agValue = ((IntField) tup.getField(afield)).getValue();

        switch (op) {
            case MIN:
                if (values == null) {
                    values = new Integer[1];
                    values[0] = agValue;
                }
                else if (agValue < values[0])
                    values[0] = agValue;
                break;
            case MAX:
                if (values == null) {
                    values = new Integer[1];
                    values[0] = agValue;
                }
                else if (agValue > values[0])
                    values[0] = agValue;
                break;
            case AVG:
            case SUM_COUNT:
            case SC_AVG:
                if (values == null) {
                    values = new Integer[2];
                    values[0] = agValue;
                    values[1] = 1;
                } else {
                    values[0] += agValue;
                    values[1]++;
                }
                break;
            case SUM:
                if (values == null) {
                    values = new Integer[1];
                    values[0] = agValue;
                }
                else
                    values[0] += agValue;
                break;
            case COUNT:
                if (values == null) {
                    values = new Integer[1];
                    values[0] = 1;
                }
                else
                    values[0]++;
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + op);
        }

        map.put(key, values);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        List<Tuple> tuples = new ArrayList<>();
        TupleDesc td = null;
        int valueField = 0;

        // create tuple description
        List<Type> types = new ArrayList<>();
        List<String> fields = new ArrayList<>();
        if (gbfield != NO_GROUPING) {
            types.add(gbfieldtype);
            fields.add("groupValue");
            valueField = 1;
        }
        types.add(Type.INT_TYPE);
        fields.add("aggregateValue");
        if (op == Op.SUM_COUNT || op == Op.SC_AVG) {
            types.add(Type.INT_TYPE);
            fields.add("aggregateValue2");
        }
        td = new TupleDesc(types.toArray(new Type[0]), fields.toArray(new String[0]));

        // create tuple list
        for (Map.Entry<Field, Integer[]> entry : map.entrySet()) {
            Tuple tuple = new Tuple(td);
            Integer[] values = entry.getValue();

            if (gbfield != NO_GROUPING)
                tuple.setField(0, entry.getKey());

            switch (op) {
                case AVG:
                    tuple.setField(valueField, new IntField(values[0] / values[1]));
                    break;
                case SUM_COUNT:
                case SC_AVG:
                    // todo
                    tuple.setField(valueField, new IntField(values[0]));
                    tuple.setField(valueField + 1, new IntField(values[0]));
                    break;
                default:
                    tuple.setField(valueField, new IntField(values[0]));
            }

            tuples.add(tuple);
        }
        return new TupleIterator(td, tuples);
    }

}
