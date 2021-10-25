package simpledb;

import java.util.Iterator;

public class HeapFileIterator extends AbstractDbFileIterator {
    private int pageIndex;
    private final TransactionId tid;
    private final int tableId, pageNum;
    private Iterator<Tuple> iterator;


    public HeapFileIterator(int tableId, TransactionId tid, int pageNum) {
        this.tid = tid;
        this.tableId = tableId;
        this.pageNum = pageNum;
        pageIndex = 0;
    }

    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        if (iterator == null)
            return null;

        if (iterator.hasNext())
            return iterator.next();

        if (pageIndex < pageNum - 1) {
            pageIndex++;
            open();
            return readNext();
        }

        return null;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        PageId pid = new HeapPageId(tableId, pageIndex);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
        iterator = page.iterator();
    }

    @Override
    public void close() {
        super.close();
        iterator = null;
        pageIndex = 0;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }
}
