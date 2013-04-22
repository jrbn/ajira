package nl.vu.cs.ajira.utils;

public class Consts {

	/********** DEFAULT VALUES **********/
	public static final double MAX_MEMORY_TO_USE = 0.75; // 75%
	public static final double MIN_MEMORY_SIZE_BEFORE_CLEANING = 0.10;
	public static final int N_ELEMENTS_TO_SAMPLE = 10;

	public static final int MAX_N_ACTIONS = 128;
	public static final int CHAIN_SIZE = 10 * 1024;

	public static final int MAXM = (int) (Runtime.getRuntime().maxMemory() / (128 * 1024 * 1024));

	public static final int TUPLES_CONTAINER_MAX_BUFFER_SIZE = (MAXM > 512 ? 512
			: MAXM > 256 ? 256 : MAXM > 128 ? 128 : MAXM > 64 ? 64 : 32) * 1024 * 1024;
	public static final int SIZE_BUFFERS_CHAIN_SEND = 4 * 1024 * 1024;
	public static final int SIZE_BUFFERS_CHAINS_PROCESS = (MAXM > 64 ? 64 : 32) * 1024 * 1024;

	// Factory initialization
	public static final int STARTING_SIZE_FACTORY = 0;
	public static final int N_ELEMENTS_FACTORY = 10000;

	public static final int MIN_SIZE_TO_SEND = Math.min(64 * 1024 * 1024,
			TUPLES_CONTAINER_MAX_BUFFER_SIZE);
	public static final int MAX_SEGMENTS_RECEIVED = 2048;

	public static final int MAX_TUPLE_SENDERS = 8;

	public static final int STATISTICS_COLLECTION_INTERVAL = 1000;

	public static final int DATATYPE_TLONG = 0;
	public static final int DATATYPE_TSTRING = 1;
	public static final int DATATYPE_TINT = 2;
	public static final int DATATYPE_TBAG = 3;
	public static final int DATATYPE_TBOOLEAN = 4;
	public static final int DATATYPE_TBYTE = 5;
	public static final int DATATYPE_TBYTEARRAY = 6;
	public static final int DATATYPE_TINTARRAY = 7;
	public static final int DATATYPE_TSTRINGARRAY = 8;
	public static final int DATATYPE_TLONGARRAY = 9;
	public static final int DATATYPE_TBOOLEANARRAY = 10;

	public static final String STATE_OPEN = "ACTIVE";
	public static final String STATE_FINISHED = "FINISHED";
	public static final String STATE_FAILED = "FAILED";
	public static final String STATE_INIT_FAILED = "INIT_FAILED";

	/********** DEFAULT CONFIGURATION OPTIONS **********/
	public static final String N_PROC_THREADS = "threads.processing";
	public static final String N_MERGE_THREADS = "threads.merge";
	public static final String START_IBIS = "ibis.startserver";
	public static final String STATS_ENABLED = "stats.enabled";
	public static final String BUCKETCOUNTER_NAME = "BucketCounter";
	public static final String CHAINCOUNTER_NAME = "ChainCounter";

}
