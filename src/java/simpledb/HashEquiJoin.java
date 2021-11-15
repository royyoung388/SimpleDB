package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class HashEquiJoin extends Operator {

    private static final long serialVersionUID = 1L;

    private JoinPredicate predicate;
    private DbIterator iterator1, iterator2;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     *
     * @param p      The predicate to use to join the children
     * @param child1 Iterator for the left(outer) relation to join
     * @param child2 Iterator for the right(inner) relation to join
     */
    public HashEquiJoin(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
        predicate = p;
        iterator1 = child1;
        iterator2 = child2;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return predicate;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(iterator1.getTupleDesc(), iterator2.getTupleDesc());
    }

    public String getJoinField1Name() {
        // some code goes here
        return iterator1.getTupleDesc().getFieldName(predicate.getField1());
    }

    public String getJoinField2Name() {
        // some code goes here
        return iterator2.getTupleDesc().getFieldName(predicate.getField2());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        iterator1.open();
        iterator2.open();
        build();
    }

    public void close() {
        // some code goes here
        super.close();
        iterator1.close();
        iterator2.close();
        map.clear();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        iterator1.rewind();
        iterator2.rewind();
        build();
    }

    transient Iterator<Tuple> listIt = null;
    transient Tuple t2 = null;
    private Map<Field, List<Tuple>> map = new HashMap<>();

    private void build() throws TransactionAbortedException, DbException {
        map.clear();
        while (iterator1.hasNext()) {
            Tuple tuple = iterator1.next();
            List<Tuple> bucket = map.get(tuple.getField(predicate.getField1()));
            if (bucket == null) {
                bucket = new ArrayList<>();
                bucket.add(tuple);
                map.put(tuple.getField(predicate.getField1()), bucket);
            } else {
                bucket.add(tuple);
            }
        }
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, there will be two copies of the join attribute in
     * the results. (Removing such duplicate columns can be done with an
     * additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (listIt != null && listIt.hasNext())
            return mergeTuple();

        while (iterator2.hasNext()) {
            t2 = iterator2.next();
            List<Tuple> bucket = map.get(t2.getField(predicate.getField2()));
            if (bucket == null)
                continue;
            listIt = bucket.iterator();
            return fetchNext();
        }
        return null;
    }

    private Tuple mergeTuple() {
        Tuple t1 = listIt.next();

        // create new tuple
        Tuple tuple = new Tuple(getTupleDesc());
        int len1 = t1.getTupleDesc().numFields();
        int len2 = t2.getTupleDesc().numFields();
        // set tuple 1
        for (int i = 0; i < len1; i++) {
            tuple.setField(i, t1.getField(i));
        }
        // set tuple 2
        for (int i = 0; i < len2; i++) {
            tuple.setField(i + len1, t2.getField(i));
        }
        return tuple;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{iterator1, iterator2};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        iterator1 = children[0];
        iterator2 = children[1];
    }

}
