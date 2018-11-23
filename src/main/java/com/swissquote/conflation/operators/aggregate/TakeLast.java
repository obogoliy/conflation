package com.swissquote.conflation.operators.aggregate;

import java.util.Collections;

public class TakeLast<T> implements MessageHolder<T> {
	private T redisQuote;

	@Override
	public synchronized void add(T redisQuote) {
		this.redisQuote = redisQuote;
	}

	@Override
	public synchronized Iterable<T> messages() {
		return Collections.singleton(redisQuote);
	}
}

