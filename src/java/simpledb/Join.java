package simpledb;

import java.util.NoSuchElementException;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    private JoinPredicate predicate;
    private DbIterator iterator1, iterator2;
    transient Tuple curr1;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     *
     * @param p      The predicate to use to join the children
     * @param child1 Iterator for the left(outer) relation to join
     * @param child2 Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
        predicate = p;
        iterator1 = child1;
        iterator2 = child2;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return predicate;
    }

    /**
     * @return the field name of join field1. Should be quantified by
     * alias or table name.
     */
    public String getJoinField1Name() {
        // some code goes here
        return iterator1.getTupleDesc().getFieldName(predicate.getField1());
    }

    /**
     * @return the field name of join field2. Should be quantified by
     * alias or table name.
     */
    public String getJoinField2Name() {
        // some code goes here
        return iterator2.getTupleDesc().getFieldName(predicate.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     * implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(iterator1.getTupleDesc(), iterator2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        iterator1.open();
        iterator2.open();
        if (iterator1.hasNext())
            curr1 = iterator1.next();
    }

    public void close() {
        // some code goes here
        super.close();
        iterator1.close();
        iterator2.close();
        curr1 = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        iterator1.rewind();
        iterator2.rewind();
        if (iterator1.hasNext())
            curr1 = iterator1.next();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        while (iterator2.hasNext()) {
            Tuple t2 = iterator2.next();
            if (predicate.filter(curr1, t2)) {
                // create new tuple
                Tuple tuple = new Tuple(getTupleDesc());
                int len1 = curr1.getTupleDesc().numFields();
                int len2 = t2.getTupleDesc().numFields();
                // set tuple 1
                for (int i = 0; i < len1; i++) {
                    tuple.setField(i, curr1.getField(i));
                }
                // set tuple 2
                for (int i = 0; i < len2; i++) {
                    tuple.setField(i + len1, t2.getField(i));
                }
                return tuple;
            }
        }

        // tuple 2 complete, set next tuple 1
        if (iterator1.hasNext()) {
            curr1 = iterator1.next();
            iterator2.rewind();
            return fetchNext();
        }

        return null;
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
