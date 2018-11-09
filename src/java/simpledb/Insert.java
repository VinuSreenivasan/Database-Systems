package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private final TransactionId transactionId;
    private DbIterator child;
    private final int tableId;
    private boolean tupleInserted;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableId)
            throws DbException {
        // some code goes here
    	this.transactionId = t;
    	this.child = child;
    	this.tableId = tableId;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	super.open();
    	child.open();
    }

    public void close() {
        // some code goes here
    	super.close();
    	child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	close();
    	open();
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
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if (tupleInserted) {
    		return null;
    	}
    	int count = 0;
    	while (child.hasNext()) {
    		Tuple tuple = child.next();
    		try {
    			Database.getBufferPool().insertTuple(transactionId, tableId, tuple);
    			count++;
    		} catch (IOException e) {
    			e.printStackTrace();
    		}
    	}
    	Type[] type = new Type[]{Type.INT_TYPE};
    	TupleDesc td = new TupleDesc(type);
    	Tuple tuple = new Tuple(td);
    	IntField intField = new IntField(count);
    	tuple.setField(0, intField);
    	this.tupleInserted = true;
        return tuple;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
    	DbIterator[] temp = new DbIterator[1];
    	temp[0] = child;
        return temp;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	this.child = children[0];
    }
}
