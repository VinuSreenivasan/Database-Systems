package simpledb;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    private final Map<Field, Integer> fieldMap;
    private final Map<Field, Integer> countMap;
    private final static StringField NO_GROUP_KEY = new StringField("no group", 2);

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.what = what;
    	this.fieldMap = new HashMap<>();
    	this.countMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	if (gbfield == NO_GROUPING) {
    		IntField tupleField = (IntField)tup.getField(afield);
    		int tupleValue = tupleField.getValue();
    		if (fieldMap.containsKey(NO_GROUP_KEY)) {
    			int val = aggregateGroup(what, fieldMap.get(NO_GROUP_KEY), tupleValue);
    			fieldMap.put(NO_GROUP_KEY, val);
    			countMap.put(NO_GROUP_KEY, countMap.get(NO_GROUP_KEY)+1);
    		} else {
    			fieldMap.put(NO_GROUP_KEY, tupleValue);
    			countMap.put(NO_GROUP_KEY, 1);
    		}
    	} else {
    		if (Type.INT_TYPE.equals(gbfieldtype)) {
    			IntField gbField = (IntField)tup.getField(gbfield);
    			IntField tupleField = (IntField)tup.getField(afield);
    			if (fieldMap.containsKey(gbField)) {
    				int tupleValue = tupleField.getValue();
    				int val = aggregateGroup(what, fieldMap.get(gbField), tupleValue);
    				fieldMap.put(gbField, val);
    				countMap.put(gbField, countMap.get(gbField)+1);
    			} else {
    				fieldMap.put(gbField, tupleField.getValue());
    				countMap.put(gbField, 1);
    			}
    		} else {
    			StringField gbField = (StringField)tup.getField(gbfield);
    			IntField tupleField = (IntField)tup.getField(afield);
    			if (fieldMap.containsKey(gbField)) {
    				int tupleValue = tupleField.getValue();
    				int val = aggregateGroup(what, fieldMap.get(gbField), tupleValue);
    				fieldMap.put(gbField, val);
    				countMap.put(gbField, countMap.get(gbField)+1);
    			} else {
    				fieldMap.put(gbField, tupleField.getValue());
    				countMap.put(gbField, 1);
    			}
    		}
    	}
    }
    
    private int aggregateGroup(Op what, Integer integer, int tupleValue) {
    	switch (what) {
		case COUNT:
			return integer + 1;
		case SUM:
			return integer + tupleValue;
		case AVG:
			return integer + tupleValue;
		case MIN:
			return Math.min(integer, tupleValue);
		case MAX:
			return Math.max(integer, tupleValue);
		default:
			throw new RuntimeException("Default case");
    	}
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        List<Tuple> tuples = new ArrayList<>();       
        if (gbfield == NO_GROUPING) {
        	TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE});
        	Tuple tuple = new Tuple(td);
        	if (!fieldMap.isEmpty()) {
        		int val = fieldMap.get(NO_GROUP_KEY);
        		int count = countMap.get(NO_GROUP_KEY);
        		if (Op.AVG.equals(what)) {
        			val = val / count;
        		}
        		if (Op.COUNT.equals(what)) {
        			val = count;
        		}
        		tuple.setField(0, new IntField(val));
        		tuples.add(tuple);
        	}
        	return new TupleIterator(td, tuples);
        } else {
        	Type[] types = new Type[2];
        	types[0] = gbfieldtype;
        	types[1] = Type.INT_TYPE;
        	TupleDesc td = new TupleDesc(types);
        	for (Field field: fieldMap.keySet()) {
        		Tuple tuple = new Tuple(td);
        		int val = fieldMap.get(field);
        		int count = countMap.get(field);
        		if (Op.AVG.equals(what)) {
        			val = val / count;
        		}
        		if (Op.COUNT.equals(what)) {
        			val = count;
        		}
        		tuple.setField(0, field);
        		tuple.setField(1, new IntField(val));
        		tuples.add(tuple);
        	}
        	return new TupleIterator(td, tuples);
        }
    }
}