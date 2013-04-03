package nl.vu.cs.ajira.mgmt;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import nl.vu.cs.ajira.storage.Factory;
import nl.vu.cs.ajira.storage.containers.WritableContainer;
import nl.vu.cs.ajira.utils.Consts;

public class MemoryManager {

	private final static MemoryManager instance = new MemoryManager();

	private final List<Factory<? extends WritableContainer<?>>> factories = new ArrayList<Factory<? extends WritableContainer<?>>>();
	private final Random r = new Random();

	private long bytesUsed;
	private long bytesMax;
	private long bytesAvailable;
	private long cachedObjects;
	private long sizeCache;

	private long bytesCanBeRequested;

	private MemoryManager() {
		doHouseKeeping();
	}

	public static MemoryManager getInstance() {
		return instance;
	}

	public void doHouseKeeping() {
		// Calculate actual memory space. If memory runs out, start cleaning up
		// the factories.
		Runtime r = Runtime.getRuntime();
		bytesUsed = r.totalMemory() - r.freeMemory();
		bytesMax = r.maxMemory();
		long maxAvailableMemory = (long) (bytesMax * Consts.MAX_MEMORY_TO_USE);
		bytesAvailable = Math.min(maxAvailableMemory, bytesMax - bytesUsed);

		// Calculates the number of cached objects and their size
		cachedObjects = 0;
		sizeCache = 0;
		for (Factory<? extends WritableContainer<?>> factory : factories) {
			cachedObjects += factory.getNAvailableElements();
			sizeCache += factory.getEstimatedSizeInBytes(this.r);
		}
		sizeCache += cachedObjects * 8; // Space required to keep address to the
										// objects

		boolean cleanup = bytesAvailable < (maxAvailableMemory * Consts.MIN_MEMORY_SIZE_BEFORE_CLEANING);
		if (cleanup && sizeCache > bytesAvailable) {
			for (Factory<? extends WritableContainer<?>> factory : factories) {
				factory.clean();
			}
		}
		bytesCanBeRequested = bytesAvailable;
	}

	public void registerFactory(Factory<? extends WritableContainer<?>> factory) {
		factories.add(factory);
	}

	public int canAllocate(int bytes) {
		if (bytes < bytesCanBeRequested) {
			synchronized (this) {
				bytesCanBeRequested -= bytes;
			}
			return bytes;
		} else {
			return -1;
		}
	}

}
