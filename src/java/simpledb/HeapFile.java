package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	
	private File f;
	private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	this.f = f;
    	this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
    	return this.f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
    	return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
    	try {
    		byte[] bufferData = new byte[BufferPool.getPageSize()];
    		RandomAccessFile randomAccessFile = new RandomAccessFile(f, "r");
    		randomAccessFile.seek(pid.pageNumber()*BufferPool.getPageSize());
    		randomAccessFile.read(bufferData, 0, BufferPool.getPageSize());
    		randomAccessFile.close();
    		HeapPageId heapPageId = new HeapPageId(pid.getTableId(), pid.pageNumber());
    		HeapPage heapPage = new HeapPage(heapPageId, bufferData);
    		return heapPage;
    	} catch (IOException e) {
    		e.printStackTrace();
    	}
        throw new IllegalArgumentException("page does not exist in this file");
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        PageId pageId = page.getId();
        int pageNumber = pageId.pageNumber();
        Database.getBufferPool();
		int pageSize = BufferPool.getPageSize();
        byte[] pageData = page.getPageData();
        RandomAccessFile dbFile = new RandomAccessFile(this.f, "rws");
        dbFile.skipBytes(pageNumber * pageSize);
        dbFile.write(pageData);
        dbFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
    	int numPages = (int)(f.length()/BufferPool.getPageSize());
        return numPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
    	int totalPages = numPages();
    	ArrayList<Page> changePages = new ArrayList<Page>();
    	for (int i=0; i < totalPages; i++) {
    		HeapPageId pid = new HeapPageId(this.getId(), i);
    		HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
    		if (page.getNumEmptySlots() > 0) {
    			try {
    				page.insertTuple(t);
    				changePages.add(page);
    				return changePages;
    			} catch (Exception e) {
    				throw new DbException("cannot insert");
    			}
    		}
    	}
    	HeapPageId heapPageId = new HeapPageId(this.getId(), totalPages);
    	HeapPage heapPage = new HeapPage(heapPageId, HeapPage.createEmptyPageData());
    	heapPage.insertTuple(t);
    	this.writePage(heapPage);
    	changePages.add(heapPage);
    	return changePages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        RecordId recordId = t.getRecordId();
        PageId pageId = recordId.getPageId();
        List<Page> changePages = new ArrayList<Page>();
        if (pageId.getTableId() == this.getId()) {
        	HeapPage heapPage = (HeapPage)Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        	heapPage.deleteTuple(t);
        	changePages.add(heapPage);
        	return (ArrayList<Page>) changePages;
        }
        throw new DbException("Record id doesn't match");
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }
    
    class HeapFileIterator implements DbFileIterator {
    	private HeapFile heapFile;
    	private TransactionId tid;
    	private Iterator<Tuple> iter;
    	private int pageNum = 0;
    	private int openFlag = 0;
    	
    	public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
    		this.heapFile = heapFile;
    		this.tid = tid;
    		this.iter = null;
    	}
    	
    	@Override
    	public void open() throws DbException, TransactionAbortedException {
    		pageNum = 0;
    		getTupleIterator();
    		openFlag = 1;
    	}
    	
    	@Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
    		if (openFlag == 0) {
    			return false;
    		} else if (iter != null && iter.hasNext()) {
    			return true;
    		} else {
    			while ((iter == null || !iter.hasNext()) && pageNum < heapFile.numPages()) {
    				getTupleIterator();
    			}
    			return (iter != null && iter.hasNext());
    		}
    	}
    	
    	@Override
    	public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
    		if (hasNext()) {
    			return iter.next();
    		} else {
    			throw new NoSuchElementException("no more tuples");
    		}
    	}
    	
    	@Override
    	public void rewind() throws DbException, TransactionAbortedException {
    		close();
    		open();
    	}
    	
    	@Override
    	public void close() {
    		openFlag = 0;
    		iter = null;
    	}
    	
    	public void getTupleIterator() throws DbException, TransactionAbortedException {
    		PageId pageId = new HeapPageId(heapFile.getId(), pageNum++);
    		HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
    		iter = heapPage.iterator();
    	}
    }

}

