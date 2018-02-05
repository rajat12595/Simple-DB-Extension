package simpledb;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An {@code IntAggregator} computes some aggregate value over a set of {@code IntField}s.
 */
public class IntAggregator implements Aggregator {

	/**
	 * Constructs an {@code Aggregate}.
	 * 
	 * @param gbfield
	 *            the 0-based index of the group-by field in the tuple, or {@code NO_GROUPING} if there is no grouping
	 * @param gbfieldtype
	 *            the type of the group by field (e.g., {@code Type.INT_TYPE}), or {@code null} if there is no grouping
	 * @param afield
	 *            the 0-based index of the aggregate field in the tuple
	 * @param what
	 *            the aggregation operator
	 */
	 
	 int gbfeild;
	 Type gbfeildtype;
	 int afeild;
	 Op what1;
	 Boolean isgrouping;
	 Map<Field, Integer> aggregratevalue;
	 Field NO_GROUPING_KEY = new StringField("No grouping",11);

	public IntAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		// some code goes here
		isgrouping = (gbfeild != NO_GROUPING);
		afeild=afield;
		gbfeild=gbfield;
		gbfeildtype=gbfieldtype;
		what1=what;
		aggregratevalue=new HashMap<Field, Integer>();

	}

	private int getDefaultValue() {
		switch (what1)
		{
		case AVG:
		case COUNT:
		case SUM:
			return 0;
		case MIN:
			return Integer.MAX_VALUE;
		case MAX:
			return Integer.MIN_VALUE;
		default:
			throw new UnsupportedOperationException();
		}
	}
	
	
	private int getAggregateValue(Field key) {
		return getValue(aggregratevalue, key);
		
		
	}

	private int getValue(Map<Field, Integer> map, Field key) {
		Integer aggregateValue = map.get(key);
		return (aggregateValue == null) ? getDefaultValue() : aggregateValue.intValue();
		
	}
	
	
	

	/**
	 * Merges a new {@code Tuple} into the aggregate, grouping as indicated in the constructor.
	 * 
	 * @param tup
	 *            the {@code Tuple} containing an aggregate field and a group-by field
	 */
	public void merge(Tuple tup) {
		// some code goes here
		int newvalue=0;
		int c1=0;
		int s1=0;
		Field gbfild= tup.getField(gbfeild);
		Integer value=getAggregateValue(gbfild);
		
		
		int tupleValue = ((IntField) tup.getField(afeild)).getValue();
		
	
		switch (what1) {
		case COUNT:
			newvalue = value+1;
			
			break;
			
		case MIN:
			newvalue = (value < tupleValue) ? value : tupleValue ;
			break;
			
		case MAX:
			newvalue = (value > tupleValue) ? value : tupleValue ;
			break;
			
		case SUM:
			newvalue = value+tupleValue ;
			break;
			
		case AVG:
			c1=value+1;
			s1=value+tupleValue;
			newvalue =(s1/c1);
		default:
			break;
		}
		aggregratevalue.put(gbfild, new Integer(newvalue));
	}

	/**
	 * Creates a {@code DbIterator} over group aggregate results.
	 *
	 * @return a {@code DbIterator} whose tuples are the pair ({@code groupVal}, {@code aggregateVal}) if using group,
	 *         or a single ({@code aggregateVal}) if no grouping. The {@code aggregateVal} is determined by the type of
	 *         aggregate specified in the constructor.
	 */
	public DbIterator iterator() {
		// some code goes here
		//throw new UnsupportedOperationException("implement me");
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
			
			
			for (Field gbvalue : aggregratevalue.keySet()) 
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


