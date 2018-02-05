package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * A {@code StringAggregator} computes some aggregate value over a set of {@code StringField}s.
 */
public class StringAggregator implements Aggregator {
	
	int gbfeild;
	Type gbfeildtype;
	int afeild;
	Op what1;
	boolean isgrouping;
	Map<Field, Integer> counts;
	Field NO_GROUPING_KEY = new StringField("No grouping", 11);

	
	/**
	 * Constructs a {@code StringAggregator}.
	 * 
	 * @param gbfield
	 *            the 0-based index of the group-by field in the tuple, or {@code NO_GROUPING} if there is no grouping
	 * @param gbfieldtype
	 *            the type of the group by field (e.g., {@code Type.INT_TYPE}), or {@code null} if there is no grouping
	 * @param afield
	 *            the 0-based index of the aggregate field in the tuple
	 * @param what
	 *            aggregation operator to use -- only supports {@code COUNT}
	 * @throws IllegalArgumentException
	 *             if {@code what != COUNT}
	 */

	public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		// some code goes here
		isgrouping =gbfield == NO_GROUPING;
		if (what != Op.COUNT)
		{
			throw new IllegalArgumentException("Illegal argument");
		}
		gbfeild=gbfield;
		gbfeildtype=gbfieldtype;
		afeild=afield;
		what1=what;
		counts = new HashMap<Field, Integer>();
	}

	/**
	 * Merges a new tuple into the aggregate, grouping as indicated in the constructor.
	 * 
	 * @param tup
	 *            the Tuple containing an aggregate field and a group-by field
	 *            
	 */
	
	private int getAggregateValue(Field key) {
		return getValue(counts, key);
		
		
	}

	private int getValue(Map<Field, Integer> map, Field key) {
		Integer aggregateValue = map.get(key);
		return (aggregateValue == null) ? 0 : aggregateValue.intValue();
		
	}

	
	public void merge(Tuple tup) {
		// some code goes here
		int newvalue=0;
		Field gbfild= tup.getField(gbfeild);
		Integer value=getAggregateValue(gbfild);		
		switch (what1) {
		case COUNT:
			newvalue = value + 1;
			
			break;
		default:
		}
		counts.put(gbfild, new Integer(newvalue));
	}

	/**
	 * Creates a {@code DbIterator} over group aggregate results.
	 *
	 * @return a {@code DbIterator} whose tuples are the pair ({@code groupVal}, {@code aggregateVal}) if using group,
	 *         or a single ({@code aggregateVal}) if no grouping. The aggregateVal is determined by the type of
	 *         aggregate specified in the constructor.
	 */
	public DbIterator iterator() {
		// some code goes here
		TupleDesc resultDesc;
		if (isgrouping)
		{
			resultDesc = new TupleDesc(new Type[] { gbfeildtype,Type.INT_TYPE }, new String[] { "GROUPED BY",what1.toString() });
		}
		
		else
		{
			resultDesc = new TupleDesc(new Type[] { Type.INT_TYPE },new String[] { what1.toString() });
		}
		
		Iterable<Tuple> tuples = getTuples(resultDesc);
		
		return new TupleIterator(resultDesc, tuples);
}

	
	public Iterable<Tuple> getTuples(TupleDesc resultDesc) {
		// TODO Auto-generated method stub
		
		List<Tuple> tuples = new ArrayList<Tuple>();
		
		if (isgrouping) 
		{
			
			
			for (Field gbvalue : counts.keySet()) 
			{
				int aggregateValue = getAggregateValue(gbvalue);
				Tuple tuple = new Tuple(resultDesc);
				tuple.setField(0, gbvalue);
				tuple.setField(1, new IntField(aggregateValue));
				tuples.add(tuple);
			}
			return tuples;
		}
		
		
		Tuple tuple = new Tuple(resultDesc);
		int aggregateValue = getAggregateValue(NO_GROUPING_KEY);
		tuple.setField(0, new IntField(aggregateValue));
		tuples.add(tuple);
		return tuples;
	}
	
}