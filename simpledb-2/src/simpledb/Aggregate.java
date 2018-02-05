package simpledb;

import java.util.*;

/**
 * An {@code Aggregate} operator computes an aggregate value (e.g., sum, avg, max, min) over a single column, grouped by a
 * single column.
 */
public class Aggregate extends AbstractDbIterator {
	
	
	/**
	 * Constructs an {@code Aggregate}.
	 *
	 * Implementation hint: depending on the type of afield, you will want to construct an {@code IntAggregator} or
	 * {@code StringAggregator} to help you with your implementation of {@code readNext()}.
	 * 
	 *
	 * @param child
	 *            the {@code DbIterator} that provides {@code Tuple}s.
	 * @param afield
	 *            the column over which we are computing an aggregate.
	 * @param gfield
	 *            the column over which we are grouping the result, or -1 if there is no grouping
	 * @param aop
	 *            the {@code Aggregator} operator to use
	 */
	boolean isGrouping;
	boolean start;
	DbIterator child;
	int afield;
	int gfeild;
	Aggregator.Op aop;
	Aggregator aggregator;
	DbIterator aggregate_result; 
	
	public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
		// some code goes here
		this.start=false;
		this.child=child;
		this.afield=afield;
		this.gfeild=gfield;
		this.aop=aop;
		this.isGrouping = gfield != Aggregator.NO_GROUPING;
	}

	public static String aggName(Aggregator.Op aop) {
		switch (aop) {
		case MIN:
			return "min";
		case MAX:
			return "max";
		case AVG:
			return "avg";
		case SUM:
			return "sum";
		case COUNT:
			return "count";
		}
		return "";
	}

	public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
		// some code goes here
		
		Type groupByFieldType = isGrouping ? child.getTupleDesc().getType(gfeild) : null;
		switch (child.getTupleDesc().getType(afield)) {
		case INT_TYPE:
			this.aggregator = new IntAggregator (gfeild,groupByFieldType, afield, aop);
			break;
		case STRING_TYPE:
			this.aggregator = new StringAggregator(gfeild,groupByFieldType, afield, aop);
			break;
		default:
			throw new UnsupportedOperationException();
		}
		
		
		this.start=true;
		child.open();
		while (child.hasNext())
		{
			aggregator.merge(child.next());
		}
		child.close();
		TupleDesc td1= getTupleDesc();
		aggregate_result = new TupleIterator(td1, aggregator.getTuples(td1)); 
		aggregate_result.open();
		
	}

	/**
	 * Returns the next {@code Tuple}. If there is a group by field, then the first field is the field by which we are
	 * grouping, and the second field is the result of computing the aggregate, If there is no group by field, then the
	 * result tuple should contain one field representing the result of the aggregate. Should return {@code null} if
	 * there are no more {@code Tuple}s.
	 */
	protected Tuple readNext() throws TransactionAbortedException, DbException {
		// some code goes here
		if(aggregate_result.hasNext())
		{
			return aggregate_result.next();
		}
		return null;
	}

	public void rewind() throws DbException, TransactionAbortedException {
		// some code goes here
		if(!start)
		{
			throw new IllegalStateException();
		}
		aggregate_result.rewind();
	}

	/**
	 * Returns the {@code TupleDesc} of this {@code Aggregate}. If there is no group by field, this will have one field
	 * - the aggregate column. If there is a group by field, the first field will be the group by field, and the second
	 * will be the aggregate value column.
	 * 
	 * The name of an aggregate column should be informative. For example:
	 * {@code aggName(aop) (child_td.getFieldName(afield))} where {@code aop} and {@code afield} are given in the
	 * constructor, and {@code child_td} is the {@code TupleDesc} of the child iterator.
	 */
	public TupleDesc getTupleDesc() {
		// some code goes here
		
		return child.getTupleDesc();
	}

	public void close() {
		this.start=false;
		if (aggregate_result != null)
			aggregate_result.close();
		// some code goes here
	}
}
