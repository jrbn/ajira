package nl.vu.cs.ajira.utils;

public class Consts {

	/********** DEFAULT VALUES **********/
	public static final int MAX_N_ACTIONS = 128;
	public static final int CHAIN_SIZE = 10 * 1024;

	public static final int SIZE_BUFFERS_CHAIN_SEND = 4 * 1024 * 1024;
	public static final int SIZE_BUFFERS_CHAINS_PROCESS = 64 * 1024 * 1024;
	public static final int TUPLES_CONTAINER_BUFFER_SIZE = 128 * 1024 * 1024;

	public static final int STARTING_SIZE_FACTORY = 0;

	public static final int MIN_SIZE_TO_SEND = 128 * 1024 * 1024;
	public static final int N_ELEMENTS_FACTORY = 100000;

	public static final int MAX_SEGMENTS_RECEIVED = 2048;

	public static final int MAX_TUPLE_SENDERS = 8;

	public static final int STATISTICS_COLLECTION_INTERVAL = 1000; // In ms. Not
																	// too low,
																	// it may
																	// affect
																	// performance.

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

	public static final int DEFAULT_INPUT_LAYER_ID = 0;
	public static final int BUCKET_INPUT_LAYER_ID = 1;
	public static final int DUMMY_INPUT_LAYER_ID = 2;
	public static final int SPLITS_INPUT_LAYER = 3;
	public static final int MAX_N_INPUT_LAYERS = 4;

	/********** DEFAULT CONFIGURATION OPTIONS **********/
	public static final String STORAGE_IMPL = "storage.impl";
	public static final String N_PROC_THREADS = "threads.processing";
	public static final String N_MERGE_THREADS = "threads.merge";
	public static final String START_IBIS = "ibis.startserver";
	public static final String STATS_ENABLED = "stats.enabled";
	public static final String BUCKETCOUNTER_NAME = "BucketCounter";
	public static final String CHAINCOUNTER_NAME = "ChainCounter";

}
