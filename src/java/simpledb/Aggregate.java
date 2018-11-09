package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private DbIterator child;
    private DbIterator aggregateIter;
    private final int afield;
    private final int gfield;
    private final Aggregator.Op aop;
    private Aggregator aggregator;
    
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
    	this.child = child;
    	this.afield = afield;
    	this.gfield = gfield;
    	this.aop = aop;
    	
    	Type gfieldtype;
    	int gfieldGrouping;
    	if (this.gfield == -1) {
    		gfieldGrouping = Aggregator.NO_GROUPING;
    		gfieldtype = Type.INT_TYPE;
    	} else {
    		gfieldtype = child.getTupleDesc().getFieldType(gfield);
    		gfieldGrouping = this.gfield;
    	}
    	
    	Type afieldtype = child.getTupleDesc().getFieldType(afield);
    	switch (afieldtype) {
    	case INT_TYPE:
    		this.aggregator = new IntegerAggregator(gfieldGrouping, gfieldtype, afield, aop);
    		break;
    	case STRING_TYPE:
    		this.aggregator = new StringAggregator(gfieldGrouping, gfieldtype, afield, aop);
    		break;
    	default:
    		throw new UnsupportedOperationException("default case");
    	}
    	
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
    	return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
    	if (gfield == Aggregator.NO_GROUPING) {
    		return null;
    	} else {
    		return getTupleDesc().getFieldName(gfield);
    	}
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
    	return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
    	return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
    	return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
    	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	// some code goes here
    	super.open();
    	child.open();
    	while (child.hasNext()) {
    		aggregator.mergeTupleIntoGroup(child.next());
    	}
    	child.close();
    	DbIterator iterator = (DbIterator)aggregator.iterator();
    	iterator.open();
    	List<Tuple> tuples = new ArrayList<Tuple>();
    	while(iterator.hasNext()) {
    		tuples.add(iterator.next());
    	}
    	this.aggregateIter = new TupleIterator(getTupleDesc(), tuples);
    	aggregateIter.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
    	if (aggregateIter.hasNext()) {
    		return aggregateIter.next();
    	} else {
    		return null;
    	}
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
    	aggregateIter.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
    	String value = aop.toString()+"("+child.getTupleDesc().getFieldName(afield)+")";
    	if (gfield == Aggregator.NO_GROUPING) {
    		return new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{value});
    	} else {
    		Type[] types = new Type[2];
    		types[0] = child.getTupleDesc().getFieldType(gfield);
    		types[1] = Type.INT_TYPE;
    		String[] strings = new String[2];
    		strings[0] = child.getTupleDesc().getFieldName(gfield);
    		strings[1] = value;
    		return new TupleDesc(types, strings);
    	}
    }

    public void close() {
	// some code goes here
    	super.close();
    	if (aggregateIter != null) {
    		aggregateIter.close();
    	}
    }

    @Override
    public DbIterator[] getChildren() {
	// some code goes here
    	return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
	// some code goes here
    	this.child = children[0];
    	Type gfieldtype;
    	int gfieldGrouping;
    	if (this.gfield == -1) {
    		gfieldtype = Type.INT_TYPE;
    		gfieldGrouping = Aggregator.NO_GROUPING;
    	} else {
    		gfieldtype = child.getTupleDesc().getFieldType(gfield);
    		gfieldGrouping = this.gfield;
    	}
    	Type afieldtype = child.getTupleDesc().getFieldType(afield);
    	switch (afieldtype) {
    	case INT_TYPE:
    		this.aggregator = new IntegerAggregator(gfieldGrouping, gfieldtype, afield, aop);
    		break;
    	case STRING_TYPE:
    		this.aggregator = new StringAggregator(gfieldGrouping, gfieldtype, afield, aop);
    		break;
    	default:
    		throw new UnsupportedOperationException("Default case");
    	}
    }
}