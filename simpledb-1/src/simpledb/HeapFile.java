package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples in no particular order. Tuples are
 * stored on pages, each of which is a fixed size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

	/**
	 * The File associated with this HeapFile.
	 */
	protected File file;

	/**
	 * The TupleDesc associated with this HeapFile.
	 */
	protected TupleDesc td;

	/**
	 * Constructs a heap file backed by the specified file.
	 * 
	 * @param f
	 *            the file that stores the on-disk backing store for this heap file.
	 */
	public HeapFile(File f, TupleDesc td) {
		this.file = f;
		this.td = td;
	}

	/**
	 * Returns the File backing this HeapFile on disk.
	 * 
	 * @return the File backing this HeapFile on disk.
	 */
	public File getFile() {
		return file;
		
	}

	/**
	 * Returns an ID uniquely identifying this HeapFile. Implementation note: you will need to generate this tableid
	 * somewhere ensure that each HeapFile has a "unique id," and that you always return the same value for a particular
	 * HeapFile. We suggest hashing the absolute file name of the file underlying the heapfile, i.e.
	 * f.getAbsoluteFile().hashCode().
	 * 
	 * @return an ID uniquely identifying this HeapFile.
	 */
	public int getId() {
		return file.getAbsoluteFile().hashCode();
	}

	/**
	 * Returns the TupleDesc of the table stored in this DbFile.
	 * 
	 * @return TupleDesc of this DbFile.
	 */
	public TupleDesc getTupleDesc() {
		return td;
	}

	// see DbFile.java for javadocs
	public Page readPage(PageId pid) {
		RandomAccessFile raf;
		int pageLocation=pid.pageno()*BufferPool.PAGE_SIZE;
		
		try
		{
			raf=new RandomAccessFile(file,"r");
			raf.seek(pageLocation);
			byte[] pageBytes=new byte[BufferPool.PAGE_SIZE];
			raf.read(pageBytes);
			raf.close();
			HeapPage page = new HeapPage((HeapPageId) pid,pageBytes);
		    return page;
		}
		catch(Exception e)
		{
			return null;
		}

		//throw new UnsupportedOperationException("Implement this");
	}


	// see DbFile.java for javadocs
	public void writePage(Page page) throws IOException {
		// some code goes here
		// not necessary for assignment1
	}

	/**
	 * Returns the number of pages in this HeapFile.
	 */
	public int numPages() {
		return (int) (file.length() / BufferPool.PAGE_SIZE);
	}

	// see DbFile.java for javadocs
	public ArrayList<Page> addTuple(TransactionId tid, Tuple t) throws DbException, IOException,
			TransactionAbortedException {
		// some code goes here
		// not necessary for assignment1
		return null;
	}

	// see DbFile.java for javadocs
	public Page deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
		// some code goes here
		// not necessary for assignment1
		return null;
	}

	// see DbFile.java for javadocs
	public DbFileIterator iterator(TransactionId tid) {
		// Implement the iterator as an anonymous class to avoid additional files.
	 return new DbFileIterator() {

		// Variables to keep the state of the iterator.
	 private int currentPageNo = 0;
	 private Page currentPage = null;
	 private PageId currentPageId = null;
	 private Iterator<Tuple> tuples = null;
	 private TransactionId tid = null;
	 
		// Method for setting the transaction id of the iterator.
	 private DbFileIterator setTid(TransactionId tid) {
	 this.tid = tid;
	 return this;
	 }
	 
		@Override
		public void open() throws DbException, TransactionAbortedException {
		// Initialize the state.
		int tableId = HeapFile.this.getId();
		this.currentPageId = new HeapPageId(tableId, this.currentPageNo);
		this.currentPage = Database.getBufferPool().getPage(tid, this.currentPageId, null);
		this.tuples = ((HeapPage) this.currentPage).iterator();
		}

		@Override
		public boolean hasNext() throws DbException,
		TransactionAbortedException {
		if (this.tuples != null) {
		// If the tuple we hold is null, then the iterotor is closed.
		// If not, keep going.
		if (this.tuples.hasNext()) {
		// If tuples has more items, return true.
		return true;
		} else {
		if (this.currentPageNo <  numPages()-1) {
		// If current held tuple is full, check if current page is the last page.
		// If not, move to next page and recursively call this function.
		int tableId = HeapFile.this.getId();
		this.currentPageNo++;
		this.currentPageId = new HeapPageId(tableId, this.currentPageNo);
		this.currentPage = Database.getBufferPool().getPage(tid, this.currentPageId, null);
		this.tuples = ((HeapPage) this.currentPage).iterator();
		return this.hasNext();
		}
		}
		}
		return false;
		}

		@Override
		public Tuple next() throws DbException,
		TransactionAbortedException, NoSuchElementException {
		if (this.tuples != null) {
		return this.tuples.next();
		} else {
		throw new NoSuchElementException();
		}
		}

		@Override
		public void rewind() throws DbException,
		TransactionAbortedException {
		// Simply reset our state.
		int tableId = HeapFile.this.getId();
		this.currentPageNo = 0;
		this.currentPageId = new HeapPageId(tableId, this.currentPageNo);
		this.currentPage = Database.getBufferPool().getPage(tid, this.currentPageId, null);
		this.tuples = ((HeapPage) this.currentPage).iterator();
		}

		@Override
		public void close() {
		// Invalidate the state variables.
		this.currentPageNo = 0;
		this.currentPageId = null;
		this.currentPage = null;
		this.tuples = null;
		
		}
	 
	 }.setTid(tid);
	 }


}
