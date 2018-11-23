package com.swissquote.conflation.operators.aggregate;

public interface MessageHolder<T> {

	void add(T t);

	Iterable<T> messages();
}
