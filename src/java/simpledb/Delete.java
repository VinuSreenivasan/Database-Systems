package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private final TransactionId transactionId;
    private DbIterator child;
    private boolean tupleDeleted;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	this.transactionId = t;
    	this.child = child;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
    	Type[] type = new Type[]{Type.INT_TYPE};
    	TupleDesc td = new TupleDesc(type);
        return td;
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
    	if (tupleDeleted) {
    		return null;
    	}
    	this.tupleDeleted = true;
    	int count = 0;
    	while (child.hasNext()) {
    		Tuple tuple = child.next();
    		try {
    			Database.getBufferPool().deleteTuple(transactionId, tuple);
    			count++;
    		} catch (IOException e) {
    			e.printStackTrace();
    		}
    	}
    	Tuple tuple = new Tuple(this.getTupleDesc());
    	IntField intField = new IntField(count);
    	tuple.setField(0, intField);
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