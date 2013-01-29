package nl.vu.cs.ajira.utils;

public class Consts {

	/********** DEFAULT VALUES **********/
	public static final int MAX_N_ACTIONS = 128;
	public static final int CHAIN_SIZE = 10 * 1024;
	public static final int CHAIN_RESERVED_SPACE = 39;

	public static final int SIZE_BUFFERS_CHILDREN_CHAIN_RESOLVE = 4 * 1024 * 1024;
	public static final int SIZE_BUFFERS_CHILDREN_CHAIN_PROCESS = 4 * 1024 * 1024;
	public static final int SIZE_BUFFERS_TUPLES_CHAIN_PROCESS = 16 * 1024 * 1024;

	public static final int SIZE_BUFFERS_CHAIN_SEND = 4 * 1024 * 1024;
	public static final int SIZE_BUFFERS_CHAIN_TERMINATED = 4 * 1024 * 1024;
	public static final int SIZE_BUFFERS_TUPLES_REQUESTED = 2 * 1024 * 1024;

	public static final int SIZE_BUFFERS_CHAINS_RESOLVE = 64 * 1024 * 1024;
	public static final int SIZE_BUFFERS_CHAINS_PROCESS = 64 * 1024 * 1024;

	public static final int TUPLES_CONTAINER_BUFFER_SIZE = 128 * 1024 * 1024;

	public static final int STARTING_SIZE_FACTORY = 0;
	public static final int INIT_ACTION_CACHE = 3;

	public static final int MIN_SIZE_TO_SEND = 4 * 1024 * 1024;
	public static final int N_ELEMENTS_FACTORY = 100000;

	public static final int MAN_N_DATA_IN_TUPLE = 20;
	public static final int MAX_SEGMENTS_RECEIVED = 2048;

	public static final int MAX_TUPLE_SENDERS = 8;

	public static final int DATATYPE_TLONG = 0;
	public static final int DATATYPE_TSTRING = 1;
	public static final int DATATYPE_TINT = 2;
	public static final int DATATYPE_TBAG = 3;
	public static final int DATATYPE_TBOOLEAN = 4;
	public static final int DATATYPE_TBYTE = 5;
	public static final int DATATYPE_TBYTEARRAY = 6;
	public static final int DATATYPE_TINTARRAY = 7;
	public static final int DATATYPE_TSTRINGARRAY = 8;

	public static final String STATE_OPEN = "OPEN";
	public static final String STATE_RESULTS_RECEIVED = "RESULTS_RECEIVED";
	public static final String STATE_FINISHED = "FINISHED";
	public static final String STATE_INIT_FAILED = "INIT_FAILED";

	public static final int DEFAULT_INPUT_LAYER_ID = 0;
	public static final int BUCKET_INPUT_LAYER_ID = 1;
	public static final int DUMMY_INPUT_LAYER_ID = 2;
	public static final int SPLITS_INPUT_LAYER = 3;
	public static final int MAX_N_INPUT_LAYERS = 4;

	public static final int MAX_N_PARAMS = 1024;
	public static final int MAX_CONCURRENT_TRANSFERS = 128;

	public static final int DEFAULT_STATISTICAL_INTERVAL = 1000;

	/********** DEFAULT CONFIGURATION OPTIONS **********/
	public static final String STORAGE_IMPL = "storage.impl";
	public static final String N_RES_THREADS = "threads.resolution";
	public static final String N_PROC_THREADS = "threads.processing";
	public static final String N_MERGE_THREADS = "threads.merge";
	public static final String START_IBIS = "ibis.startserver";
	public static final String STATISTICAL_INTERVAL = "arch.stats";
	public static final String STATS_ENABLED = "arch.stats.enabled";
	public static final String DICT_DIR = "dictionary.dir";
	public static final String BUCKETCOUNTER_NAME = "BucketCounter";
	public static final String CHAINCOUNTER_NAME = "ChainCounter";

}
