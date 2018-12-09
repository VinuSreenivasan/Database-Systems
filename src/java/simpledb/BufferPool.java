package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


class LockManager {
		
	private final HashMap<PageId, HashSet<TransactionId>> sharedLocks;
	private final HashMap<PageId, TransactionId> exclusiveLocks;
	private final HashMap<TransactionId, HashSet<PageId>> sharedPages;
	private final HashMap<TransactionId, HashSet<PageId>> exclusivePages;
	private final long sleepTime;
	private final long timeOutTime;
	
	public LockManager(long sleepTime, long timeOutTime) {
		this.exclusiveLocks = new HashMap<PageId, TransactionId>();
		this.sharedLocks =  new HashMap<PageId, HashSet<TransactionId>>();
		this.sharedPages = new HashMap<TransactionId, HashSet<PageId>>();
		this.exclusivePages = new HashMap<TransactionId, HashSet<PageId>>();	
		this.sleepTime = sleepTime;
		this.timeOutTime = timeOutTime;
	}
	
	public LockManager() {
		this.exclusiveLocks = new HashMap<PageId, TransactionId>();
		this.sharedLocks =  new HashMap<PageId, HashSet<TransactionId>>();
		this.sharedPages = new HashMap<TransactionId, HashSet<PageId>>();
		this.exclusivePages = new HashMap<TransactionId, HashSet<PageId>>();
		this.sleepTime = 20;
		this.timeOutTime = 200;
	}
	
	public synchronized boolean acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
		if (perm.equals(Permissions.READ_ONLY)) {
			long start = System.currentTimeMillis();
			while(!acquireReadLock(tid, pid)) {
				try {
					Random rand = new Random();
					Thread.sleep(this.sleepTime + rand.nextInt(10));
				} catch (Exception e){
					e.printStackTrace();
					System.out.println("Error occured while waiting");
				}
				long current = System.currentTimeMillis();
				if (current - start > this.timeOutTime) {
					releasePage(tid, pid);
					throw new TransactionAbortedException();
				}
			}
			return true;
		}
		if (perm.equals(Permissions.READ_WRITE)) {
			long start = System.currentTimeMillis();
			while(!acquireReadWriteLock(tid, pid)) {
				try {
					Random rand = new Random();
					Thread.sleep(this.sleepTime + rand.nextInt(10));
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println("Error occured while waiting");
				}		
				long current = System.currentTimeMillis();
				if (current - start > this.timeOutTime) {
					releasePage(tid, pid);
					throw new TransactionAbortedException();
				}
			}
			return true;
		}
		else {
			throw new IllegalArgumentException("Only read and read write are acceptaple");
		}
	}
	
	private boolean acquireReadLock(TransactionId tid, PageId pid) {
		// if doesn't have the exclusive lock yet
		if (!exclusiveLocks.containsKey(pid)) {	
			if (sharedLocks.containsKey(pid)) {
				sharedLocks.get(pid).add(tid);
			}
			if (!sharedLocks.containsKey(pid)) {
				HashSet<TransactionId> tids = new HashSet<>();
				tids.add(tid);
				sharedLocks.put(pid, tids);	
			}
			if (sharedPages.containsKey(tid)) {
				sharedPages.get(tid).add(pid);
			}
			if (!sharedPages.containsKey(tid)) {
				HashSet<PageId> pids = new HashSet<>();
				pids.add(pid);
				sharedPages.put(tid, pids);	
			}
			return true;
		} 
		// if it has write lock
		else {
			TransactionId exclusiveTransaction = exclusiveLocks.get(pid);
			if (exclusiveTransaction != null && exclusiveTransaction.equals(tid)) {
				return true;
			}
			else {
				return false;
			}
		}
	}
	
	private boolean acquireReadWriteLock(TransactionId tid, PageId pid) {
		TransactionId notNullTd;
		if (tid == null) {
			notNullTd = new TransactionId();
		}
		else {
			notNullTd = tid;
		}
		if (exclusiveLocks.containsKey(pid)) {
			if (exclusiveLocks.get(pid).equals(notNullTd)) {		
				return true;
			}
			return false;
		}
		else {
			if (sharedLocks.containsKey(pid)) {		
				HashSet<TransactionId> tids = sharedLocks.get(pid);
				if (tids.contains(tid)) {
					if (tids.size() == 1) {
						exclusiveLocks.put(pid, notNullTd);
						if (exclusivePages.containsKey(tid)) {
							exclusivePages.get(tid).add(pid);
						}
						else {
							HashSet<PageId> pids = new HashSet<>();
							pids.add(pid);
							exclusivePages.put(notNullTd, pids);
						}
						sharedLocks.get(pid).remove(notNullTd);
						if (sharedLocks.get(pid).size() == 0) {
							sharedLocks.remove(pid);
						}
						if (sharedPages.containsKey(notNullTd)) {
							sharedPages.get(notNullTd).remove(pid);
							if (sharedPages.get(notNullTd).size() == 0) {
								sharedPages.remove(notNullTd);
							}
						}				
						return true;
					}
					else {	
						return false;
					}
				} else {
					return false;
				}
			}
			else {
				exclusiveLocks.put(pid, notNullTd);	
				if (exclusivePages.containsKey(notNullTd)) {
					exclusivePages.get(notNullTd).add(pid);
				}
				else {
					HashSet<PageId> pids = new HashSet<PageId>();
					pids.add(pid);
					exclusivePages.put(notNullTd, pids);
				}		
				return true;
			}
		}
	}
	
	public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
		if (sharedLocks.containsKey(pid) && sharedLocks.get(pid).contains(tid)) {
			return true;
		}
		if (exclusiveLocks.containsKey(pid) && exclusiveLocks.get(pid).equals(tid)) {
			return true;
		}
		return false;
	}
	
	public synchronized void releasePage(TransactionId tid, PageId pid) {
		if (tid != null && pid != null) {
			if (sharedPages.containsKey(tid)) {
				sharedPages.get(tid).remove(pid);
			}
			if (exclusivePages.containsKey(tid)) {
				exclusivePages.get(tid).remove(pid);
			}
			if (sharedLocks.containsKey(pid)) {
				sharedLocks.get(pid).remove(tid);
			}
			if (exclusiveLocks.containsKey(pid)) {
				exclusiveLocks.remove(pid);
			}
		}
		else if (tid == null) {
			if (sharedLocks.containsKey(pid)) {		
				HashSet<TransactionId> tids = sharedLocks.get(pid);
				for (TransactionId transaction: tids) {
					if (sharedPages.containsKey(transaction)) {
						sharedPages.get(transaction).remove(pid);
						if (sharedPages.get(transaction).size() == 0) {
							sharedPages.remove(transaction);
						}
					}
				}
				sharedLocks.remove(pid);
			}
			if (exclusiveLocks.containsKey(pid)) {
				TransactionId transaction = exclusiveLocks.get(pid);	
				if (exclusivePages.containsKey(transaction)) {
					exclusivePages.get(transaction).remove(pid);
					if (exclusivePages.get(transaction).size() == 0) {
						exclusivePages.remove(transaction);
					}
				}
				exclusiveLocks.remove(pid);
			}
		}
		else if (pid == null) {	
			if (sharedPages.containsKey(tid)) {
				HashSet<PageId> pids = sharedPages.get(tid);
				for (PageId pageID: pids) {
					if (sharedLocks.containsKey(pageID)) {
						sharedLocks.get(pageID).remove(tid);
						if (sharedLocks.get(pageID).size() == 0) {
							sharedLocks.remove(pageID);
						}
					}
				}
				sharedPages.remove(tid);			
			}
			if (exclusivePages.containsKey(tid)) {
				HashSet<PageId> pids = exclusivePages.get(tid);	
				for (PageId pageID: pids) {
					if (exclusiveLocks.containsKey(pageID)) {
						exclusiveLocks.remove(pageID);
					}
				}
				exclusivePages.remove(tid);
			}
		}
	}
	

	public synchronized Set<PageId> getDirtiedPages(TransactionId tid) {
		Set<PageId> result = new HashSet<>();
		if (exclusivePages.containsKey(tid)) {
			result.addAll(exclusivePages.get(tid));
		}
		return result;
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
    	lockManager.acquireLock(tid, pid, perm);
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
    	lockManager.releasePage(tid, pid);
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
    	if (commit) {
    		Set<PageId> dirtyPages = lockManager.getDirtiedPages(tid);
    		for (PageId pid: dirtyPages) {
    			Page page = this.cacheMap.get(pid);
    			if (tid.equals(page.isDirty())) {
    				flushPage(pid);
    				page.setBeforeImage();
    			}
    		}
    	} else {
    		Set<PageId> dirtyPages = lockManager.getDirtiedPages(tid);
    		for (PageId pid: dirtyPages) {
    			cacheMap.remove(pid);
    			try {
    				getPage(tid, pid, Permissions.READ_ONLY);
    			} catch (Exception e) {
    				e.printStackTrace();
    			}
    		}
    	}
    	lockManager.releasePage(tid, null);
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
    	Set<PageId> dirtyPages = lockManager.getDirtiedPages(tid);
    	for (PageId pid: dirtyPages) {
    		flushPage(pid);
    		Page page = this.cacheMap.get(pid);
    		page.setBeforeImage();
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
		
		lockManager.releasePage(null, pid);
		cacheMap.remove(pid);
		presentNumPages.decrementAndGet();
    }
}