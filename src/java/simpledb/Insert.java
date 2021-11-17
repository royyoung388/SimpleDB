package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator iterator;
    private TransactionId tid;
    private int tableId;
    private Tuple result = null;

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableId)
            throws DbException {
        // some code goes here
        tid = t;
        iterator = child;
        this.tableId = tableId;
        result = new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"number"}));
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return result.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        iterator.open();
    }

    public void close() {
        // some code goes here
        super.close();
        iterator.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        iterator.rewind();
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
     * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (result.getField(0) != null) {
            return null;
        }

        int count = 0;
        try {
            while (iterator.hasNext()) {
                Tuple tuple = iterator.next();
                Database.getBufferPool().insertTuple(tid, tableId, tuple);
                count++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        result.setField(0, new IntField(count));

        return result;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{iterator};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        iterator = children[0];
    }
}
