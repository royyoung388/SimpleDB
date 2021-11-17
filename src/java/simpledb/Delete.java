package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private DbIterator iterator;
    private Tuple result;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
        tid = t;
        iterator = child;
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
        if (result.getField(0) != null)
            return null;

        int count = 0;
        try {
            while (iterator.hasNext()) {
                Tuple tuple = iterator.next();
                Database.getBufferPool().deleteTuple(tid, tuple);
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
