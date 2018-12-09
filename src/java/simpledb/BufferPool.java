package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

enum LockType{sharedLocks, exclusivelocks}

class LockData{
	LockType lockType;
	ArrayList<TransactionId> transactionCurPage;
	
	LockData(LockType lockType, ArrayList<TransactionId> t){
		this.lockType = lockType;
		this.transactionCurPage = t;
	}
}

class LockManager{
	ConcurrentHashMap<TransactionId, ArrayList<PageId>> xactMap = new ConcurrentHashMap<TransactionId, ArrayList<PageId>>();
	ConcurrentHashMap<PageId, LockData> pageLockMap = new ConcurrentHashMap<PageId, LockData>();
	
	private synchronized void block(long start, long timeout) throws TransactionAbortedException {
		
		if (System.currentTimeMillis() - start > timeout) {
			throw new TransactionAbortedException();
		}
		
		try {
			wait(timeout);
			if (System.currentTimeMillis() - start > timeout) {
				throw new TransactionAbortedException();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public synchronized void acquireLock(TransactionId tid, PageId pid, LockType type) throws TransactionAbortedException {
		
		long begin = System.currentTimeMillis();
		Random random = new Random();
		long timeout = random.nextInt(2000);
		
		while(true) {
			if (pageLockMap.containsKey(pid)) {
				
				if (pageLockMap.get(pid).lockType == LockType.sharedLocks) {
					
					if (type == LockType.sharedLocks) {
						
						if (!(pageLockMap.get(pid).transactionCurPage.contains(tid))) {
							pageLockMap.get(pid).transactionCurPage.add(tid);
						}
						
						updateXactMap(tid, pid);
						return;
						
					} else {
						if ((pageLockMap.get(pid).transactionCurPage.size() == 1) && (pageLockMap.get(pid).transactionCurPage.get(0) == tid) && (xactMap.containsKey(tid)) && (xactMap.get(tid).contains(pid))) {
							pageLockMap.get(pid).lockType = LockType.exclusivelocks;
							return;
						} else {
							block(begin, timeout);
						}
					}
					
				} else {
					if (pageLockMap.get(pid).transactionCurPage.get(0) != tid) {
						block(begin, timeout);
					} else {
						return;
					}
				}
				
			} else {
				updateXactMap(tid, pid);
				ArrayList<TransactionId> t = new ArrayList<TransactionId>();
				t.add(tid);
				pageLockMap.put(pid, new LockData(type, t));
				return;
			}
		}
	}
	
	private synchronized void updateXactMap(TransactionId tid, PageId pid) {
		
		if (xactMap.containsKey(tid)) {
			if (xactMap.get(tid).contains(pid)) {
				return;
			} else {
				xactMap.get(tid).add(pid);
			}
		} else {
			ArrayList<PageId> pageList = new ArrayList<PageId>();
			pageList.add(pid);
			xactMap.put(tid, pageList);
		}
	}
	
	public synchronized void releaseLock(TransactionId tid, PageId pid) {
		
		if (xactMap.containsKey(tid)) {
			xactMap.get(tid).remove(pid);
			
			if (xactMap.get(tid).size() == 0) {
				xactMap.remove(tid);
			}
		}
		
		if (pageLockMap.containsKey(pid)) {
			pageLockMap.get(pid).transactionCurPage.remove(tid);
			
			if (pageLockMap.get(pid).transactionCurPage.size() == 0) {
				pageLockMap.remove(pid);
			} else {
				notify();
			}
		}
	}
	
	public synchronized void releaseAllLocks(TransactionId tid) {
		
		if (xactMap.containsKey(tid)) {
			PageId[] pages = new PageId[xactMap.get(tid).size()];
			PageId[] pageRelease = xactMap.get(tid).toArray(pages);
			
			for (PageId pid: pageRelease) {
				releaseLock(tid, pid);
			}
		}
	}
	
	public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
		
		if (xactMap.containsKey(tid)) {
			if (xactMap.get(tid).contains(pid)) {
				return true;
			}
		}
		return false;
	}
}


/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    
    private int numPages;
    
    private ConcurrentHashMap<PageId, Page> cacheMap;
    
    private AtomicInteger presentNumPages;
    private final LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
    	this.numPages = numPages;
    	this.cacheMap = new ConcurrentHashMap<PageId, Page>();
    	this.presentNumPages = new AtomicInteger(0);
    	this.lockManager = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
    	
    	LockType lockType;
    	
    	if (perm == Permissions.READ_ONLY) {
    		lockType = LockType.sharedLocks;
    	} else {
    		lockType = LockType.exclusivelocks; 
    	}
    	
    	lockManager.acquireLock(tid, pid, lockType);
    	
    	if (cacheMap.containsKey(pid)) {
    		return cacheMap.get(pid);
    	} 
    	else {
    		if (presentNumPages.intValue() >= numPages) {
    			evictPage();
    		}
			Catalog catalog = Database.getCatalog();
			DbFile file = catalog.getDatabaseFile(pid.getTableId());
			Page page = file.readPage(pid);
			cacheMap.put(pid, page);
			presentNumPages.incrementAndGet();
			return page;
    	}
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4
    	lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4
    	transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4
    	List<PageId> xactLockedPages = lockManager.xactMap.get(tid);
    	
    	if (xactLockedPages != null) {
    		
    		for (PageId pageId: xactLockedPages) {
    			
    			if (commit) {
    				flushPage(pageId);
    			} else if (cacheMap.get(pageId) != null && cacheMap.get(pageId).isDirty() != null) {
    				discardPage(pageId);
    			}
    		}
    	}
    	
    	lockManager.releaseAllLocks(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
		Catalog catalog = Database.getCatalog();
		DbFile file = catalog.getDatabaseFile(tableId);
		ArrayList<Page> dirtyPages = file.insertTuple(tid, t);
		Iterator<Page> iter = dirtyPages.iterator();
		while (iter.hasNext()) {
			Page page = iter.next();
			page.markDirty(true, tid);
			cacheMap.put(page.getId(), page);
		}
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	int tableId = t.getRecordId().getPageId().getTableId();
		Catalog catalog = Database.getCatalog();
		DbFile file = catalog.getDatabaseFile(tableId);
		ArrayList<Page> dirtyPages = file.deleteTuple(tid, t);
		Iterator<Page> iter = dirtyPages.iterator();
		while (iter.hasNext()) {
			Page page = iter.next();
			page.markDirty(true, tid);
			cacheMap.put(page.getId(), page);
		}
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
	    Iterator<PageId> it = this.cacheMap.keySet().iterator();
	    while (it.hasNext()) {
	    	PageId pageId = it.next();
	    	flushPage(pageId);
	    }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
    	if (cacheMap.containsKey(pid)) {
			cacheMap.remove(pid);
			presentNumPages.decrementAndGet();
		}
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
    	if (cacheMap.containsKey(pid)) {
    		Page page = cacheMap.get(pid);
			TransactionId tid = page.isDirty();
			if (tid != null) {
				DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
				file.writePage(page);
				page.markDirty(false, tid);
				cacheMap.put(pid, page);
			}
		}
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4
    	if (lockManager.xactMap.containsKey(tid)) {
    		ArrayList<PageId> pagesFlush = lockManager.xactMap.get(tid);
    		for (PageId pageId : pagesFlush) {
    			flushPage(pageId);
    		}
    	}
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
    	Iterator<PageId> iter = cacheMap.keySet().iterator();
		PageId pid = null;
		
		while(iter.hasNext()) {
			pid = iter.next();
			if (cacheMap.get(pid).isDirty() == null) {
				break;
			}
		}
		
		if (pid == null || cacheMap.get(pid).isDirty() != null) {
			throw new DbException("Invalid");
		}
		
		try {
			flushPage(pid);
		} catch (Exception e) {
			e.getStackTrace();
			throw new DbException("Eviction failure");
		}
		
		cacheMap.remove(pid);
		presentNumPages.decrementAndGet();
    }
}