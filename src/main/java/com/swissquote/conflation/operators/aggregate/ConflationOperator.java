package com.swissquote.conflation.operators.aggregate;

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import lombok.RequiredArgsConstructor;
import rx.Observable;
import rx.Producer;
import rx.Scheduler;
import rx.Subscriber;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.observers.SerializedSubscriber;
import rx.schedulers.Schedulers;

@RequiredArgsConstructor
public class ConflationOperator<T, R> implements Observable.Operator<T, T> {
	private final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("Conflation-%d").build();
	private final Executor executor = Executors.newSingleThreadExecutor(threadFactory);
	private final Scheduler scheduler = Schedulers.from(executor);

	private final Func1<T, R> pkExtractor;
	private final Func0<? extends MessageHolder<T>> messageHolderFactory;

	@Override
	public Subscriber<? super T> call(Subscriber<? super T> child) {
		Subscriber<? super T> subscriber = new SerializedSubscriber<>(child);

		Scheduler.Worker worker = scheduler.createWorker();

		ConflatingSubscriber parent = new ConflatingSubscriber(subscriber, worker);
		parent.add(worker);
		child.add(parent);
		parent.scheduleFlush();
		child.setProducer(parent.createProducer());
		return parent;

	}

	@RequiredArgsConstructor
	private class ConflatingSubscriber extends Subscriber<T> {
		private final Subscriber<? super T> actual;
		private final Scheduler.Worker worker;
		private final Queue<R> queue = new LinkedBlockingDeque<>();
		private ConcurrentHashMap<R, MessageHolder<T>> map = new ConcurrentHashMap<>();
		private AtomicLong requested = new AtomicLong();
		private AtomicLong sent = new AtomicLong();

		@Override
		public void onCompleted() {
			actual.onCompleted();
		}

		@Override
		public void onError(Throwable e) {
			actual.onError(e);
			unsubscribe();
		}

		Producer createProducer() {
			return n -> {
				if (n < 0L) {
					throw new IllegalArgumentException("n >= required but it was " + n);
				}
				if (n != 0L) {
					requested.getAndUpdate(current -> current + n);
				}
			};
		}

		public void onNext(T entity) {
			map.compute(pkExtractor.call(entity), (isin, messageHolder) -> {
				if (messageHolder == null) {
					messageHolder = messageHolderFactory.call();
					messageHolder.add(entity);
					queue.add(pkExtractor.call(entity));
				} else {
					messageHolder.add(entity);
				}
				return messageHolder;
			});
		}

		void scheduleFlush() {
			worker.schedule(this::flush);
		}

		private void flush() {
			while (!Thread.currentThread().isInterrupted()) {
				if (queue.peek() != null && requested.get() >= sent.get()) {
					map.remove(queue.poll()).messages()
							.forEach(t -> {
								sent.incrementAndGet();
								actual.onNext(t);
							});
				}
			}
		}
	}

}


