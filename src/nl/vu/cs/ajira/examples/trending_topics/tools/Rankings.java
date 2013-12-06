package nl.vu.cs.ajira.examples.trending_topics.tools;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;

public class Rankings {
	private static final int DEFAULT_COUNT = 10;

	private final int maxSize;
	private final List<RankableString> rankedItems = Lists.newArrayList();

	public Rankings() {
		this(DEFAULT_COUNT);
	}

	public Rankings(int topN) {
		if (topN < 1) {
			throw new IllegalArgumentException("topN must be >= 1");
		}
		maxSize = topN;
	}

	/**
	 * @return the maximum possible number (size) of ranked objects this
	 *         instance can hold
	 */
	public int maxSize() {
		return maxSize;
	}

	/**
	 * @return the number (size) of ranked objects this instance is currently
	 *         holding
	 */
	public int size() {
		return rankedItems.size();
	}

	public List<RankableString> getRankings() {
		return defensiveCopyOf(rankedItems);
	}

	private List<RankableString> defensiveCopyOf(List<RankableString> list) {
		return Lists.newArrayList(rankedItems);
	}

	public void updateWith(Rankings other) {
		for (RankableString r : other.getRankings()) {
			updateWith(r);
		}
	}

	public void updateWith(RankableString r) {
		synchronized (rankedItems) {
			addOrReplace(r);
			rerank();
			shrinkRankingsIfNeeded();
		}
	}

	private void addOrReplace(RankableString r) {
		Integer rank = findRankOf(r);
		if (rank != null) {
			rankedItems.set(rank, r);
		} else {
			rankedItems.add(r);
		}
	}

	private Integer findRankOf(RankableString r) {
		String tag = r.getString();
		for (int rank = 0; rank < rankedItems.size(); rank++) {
			String cur = rankedItems.get(rank).getString();
			if (cur.equals(tag)) {
				return rank;
			}
		}
		return null;
	}

	private void rerank() {
		Collections.sort(rankedItems);
		Collections.reverse(rankedItems);
	}

	private void shrinkRankingsIfNeeded() {
		if (rankedItems.size() > maxSize) {
			rankedItems.remove(maxSize);
		}
	}

	/**
	 * Removes ranking entries that have a count of zero.
	 */
	public void pruneZeroCounts() {
		int i = 0;
		while (i < rankedItems.size()) {
			if (rankedItems.get(i).getCount() == 0) {
				rankedItems.remove(i);
			} else {
				i++;
			}
		}
	}

	@Override
	public String toString() {
		return rankedItems.toString();
	}
}