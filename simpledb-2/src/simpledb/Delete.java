package simpledb;  

/**
 * The delete operator.  Delete reads tuples from its child operator and
 * removes them from the table they belong to.
 */
public class Delete extends AbstractDbIterator {

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
	
	TransactionId id;
	DbIterator child1;
	boolean start;
	boolean removed;
	
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	id=t;
    	child1=child;
    	start=false;
    	removed=false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
    	
        return child1.getTupleDesc();
        
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	//super.open();
    	start=true;
    	removed=false;
    }

    public void close() {
        // some code goes here
    	super.close();
    	start=false;
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
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if(removed)
    	{
    		return null;
    	}
        child1.open();
        int counter=0;
        while(child1.hasNext())
        {
        	Tuple t=child1.next();
        	Database.getBufferPool().deleteTuple(id, t);
        	counter++;
        }
        child1.close();
        Tuple answer = new Tuple(getTupleDesc());
        answer.setField(0, new IntField(counter));
        removed=true;
                
        return answer;
    }
}
