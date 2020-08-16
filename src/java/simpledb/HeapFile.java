package simpledb;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try {
            byte[] data = new byte[BufferPool.getPageSize()];
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            raf.seek(pid.getPageNumber() * BufferPool.getPageSize());
            raf.read(data);
            raf.close();
            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.seek(page.getId().getPageNumber() * BufferPool.getPageSize());
        raf.write(page.getPageData());
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    @Override
    public int numPages() {
        // some code goes here
        int numPage = (int) file.length() / BufferPool.getPageSize();
        if (file.length() % BufferPool.getPageSize() != 0) {
            numPage++;
        }
//        System.out.printf("kkk len: %d pagesize: %d numPage: %d\n", file.length(), BufferPool.getPageSize(), numPage);
        return numPage;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        for (int i = 0; i < numPages(); i++) {
            HeapPageId pid = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, null);
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                return new ArrayList<>(Arrays.asList(page));
            }
        }
        // Create a new page, the page number will be the current numPages()
        BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(file, true));
        byte[] emptyData = HeapPage.createEmptyPageData();
        bw.write(emptyData);
        bw.close();

        HeapPageId pid = new HeapPageId(getId(), numPages() - 1);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, null);
        page.insertTuple(t);
        return new ArrayList<>(Arrays.asList(page));
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        if (t.getRecordId().getPageId().getTableId() != getId()) {
            throw new DbException(String.format("Page %s is not part of table %d", t.getRecordId().getPageId(), getId()));
        }
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), null);
        page.deleteTuple(t);
//        t.setRecordId(null);
        return new ArrayList<>(Arrays.asList(page));
    }

    private static class HeapFileIterator extends AbstractDbFileIterator {

        private final HeapFile heapFile;
        private int nextPageNum;
        private Iterator<Tuple> curPageIt;

        private HeapFileIterator(HeapFile heapFile) {
            this.heapFile = heapFile;
            close();  // Newly created iterator is "closed".
        }

        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException {
            if (curPageIt != null && curPageIt.hasNext()) {
                return curPageIt.next();
            }
            // Recursively find next page with readable tuple, or return null
            if (nextPageNum == heapFile.numPages()) {
                return null;
            }
            HeapPageId pid = new HeapPageId(heapFile.getId(), nextPageNum);
            nextPageNum++;
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(null, pid, null);
            curPageIt = page.iterator();
            return readNext();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            super.open();
            nextPageNum = 0;
        }

        @Override
        public void close() {
            curPageIt = null;
            nextPageNum = heapFile.numPages();
            super.close();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            curPageIt = null;
            nextPageNum = 0;
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this);
    }

}

