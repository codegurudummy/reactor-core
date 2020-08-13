/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable.ConditionalSubscriber;
import reactor.core.publisher.Operators.DeferredSubscription;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Uses a resource that is lazily generated by a {@link Publisher} for each individual{@link Subscriber},
 * while streaming the values from a {@link Publisher} derived from the same resource.
 * Whenever the resulting sequence terminates, the relevant {@link Function} generates
 * a "cleanup" {@link Publisher} that is invoked but doesn't change the content of the
 * main sequence. Instead it just defers the termination (unless it errors, in which case
 * the error suppresses the original termination signal).
 * <p>
 * Note that if the resource supplying {@link Publisher} emits more than one resource, the
 * subsequent resources are dropped ({@link Operators#onNextDropped(Object, Context)}). If
 * the publisher errors AFTER having emitted one resource, the error is also silently dropped
 * ({@link Operators#onErrorDropped(Throwable, Context)}).
 *
 * @param <T> the value type streamed
 * @param <S> the resource type
 */
final class FluxUsingWhen<T, S> extends Flux<T> implements SourceProducer<T> {

	final Publisher<S>                                                     resourceSupplier;
	final Function<? super S, ? extends Publisher<? extends T>>            resourceClosure;
	final Function<? super S, ? extends Publisher<?>>                      asyncComplete;
	final BiFunction<? super S, ? super Throwable, ? extends Publisher<?>> asyncError;
	@Nullable
	final Function<? super S, ? extends Publisher<?>>                      asyncCancel;

	FluxUsingWhen(Publisher<S> resourceSupplier,
			Function<? super S, ? extends Publisher<? extends T>> resourceClosure,
			Function<? super S, ? extends Publisher<?>> asyncComplete,
			BiFunction<? super S, ? super Throwable, ? extends Publisher<?>> asyncError,
			@Nullable Function<? super S, ? extends Publisher<?>> asyncCancel) {
		this.resourceSupplier = Objects.requireNonNull(resourceSupplier, "resourceSupplier");
		this.resourceClosure = Objects.requireNonNull(resourceClosure, "resourceClosure");
		this.asyncComplete = Objects.requireNonNull(asyncComplete, "asyncComplete");
		this.asyncError = Objects.requireNonNull(asyncError, "asyncError");
		this.asyncCancel = asyncCancel;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(CoreSubscriber<? super T> actual) {
		if (resourceSupplier instanceof Callable) {
			try {
				Callable<S> resourceCallable = (Callable<S>) resourceSupplier;
				S resource = resourceCallable.call();
				if (resource == null) {
					Operators.complete(actual);
				}
				else {
					Publisher<? extends T> p = deriveFluxFromResource(resource, resourceClosure);
					UsingWhenSubscriber<? super T, S> subscriber = prepareSubscriberForResource(resource,
							actual,
							asyncComplete,
							asyncError,
							asyncCancel,
							null);

					p.subscribe(subscriber);
				}
			}
			catch (Throwable e) {
				Operators.error(actual, e);
			}
			return;
		}

		//trigger the resource creation and delay the subscription to actual
		resourceSupplier.subscribe(new ResourceSubscriber(actual, resourceClosure, asyncComplete, asyncError, asyncCancel, resourceSupplier instanceof Mono));
	}

	@Override
	public Object scanUnsafe(Attr key) {
		return null; //no particular key to be represented, still useful in hooks
	}

	private static <RESOURCE, T> Publisher<? extends T> deriveFluxFromResource(
			RESOURCE resource,
			Function<? super RESOURCE, ? extends Publisher<? extends T>> resourceClosure) {

		Publisher<? extends T> p;

		try {
			p = Objects.requireNonNull(resourceClosure.apply(resource),
					"The resourceClosure function returned a null value");
		}
		catch (Throwable e) {
			p = Flux.error(e);
		}

		return p;
	}

	private static <RESOURCE, T> UsingWhenSubscriber<? super T, RESOURCE> prepareSubscriberForResource(
			RESOURCE resource,
			CoreSubscriber<? super T> actual,
			Function<? super RESOURCE, ? extends Publisher<?>> asyncComplete,
			BiFunction<? super RESOURCE, ? super Throwable, ? extends Publisher<?>> asyncError,
			@Nullable Function<? super RESOURCE, ? extends Publisher<?>> asyncCancel,
			@Nullable DeferredSubscription arbiter) {
		if (actual instanceof ConditionalSubscriber) {
			@SuppressWarnings("unchecked")
			ConditionalSubscriber<? super T> conditionalActual = (ConditionalSubscriber<? super T>) actual;
			return new UsingWhenConditionalSubscriber<>(conditionalActual,
					resource,
					asyncComplete,
					asyncError,
					asyncCancel,
					arbiter);
		}
		else {
			return new UsingWhenSubscriber<>(actual,
					resource,
					asyncComplete,
					asyncError,
					asyncCancel,
					arbiter);
		}
	}

	static class ResourceSubscriber<S, T> extends DeferredSubscription
			implements InnerConsumer<S> {

		final CoreSubscriber<? super T>                                        actual;
		final Function<? super S, ? extends Publisher<? extends T>>            resourceClosure;
		final Function<? super S, ? extends Publisher<?>>                      asyncComplete;
		final BiFunction<? super S, ? super Throwable, ? extends Publisher<?>> asyncError;
		@Nullable
		final Function<? super S, ? extends Publisher<?>>                      asyncCancel;
		final boolean                                                          isMonoSource;

		Subscription resourceSubscription;
		boolean      resourceProvided;

		UsingWhenSubscriber<? super T, S> closureSubscriber;

		ResourceSubscriber(CoreSubscriber<? super T> actual,
				Function<? super S, ? extends Publisher<? extends T>> resourceClosure,
				Function<? super S, ? extends Publisher<?>> asyncComplete,
				BiFunction<? super S, ? super Throwable, ? extends Publisher<?>> asyncError,
				@Nullable Function<? super S, ? extends Publisher<?>> asyncCancel,
				boolean isMonoSource) {
			this.actual = Objects.requireNonNull(actual, "actual");
			this.resourceClosure = Objects.requireNonNull(resourceClosure, "resourceClosure");
			this.asyncComplete = Objects.requireNonNull(asyncComplete, "asyncComplete");
			this.asyncError = Objects.requireNonNull(asyncError, "asyncError");
			this.asyncCancel = asyncCancel;
			this.isMonoSource = isMonoSource;
		}

		@Override
		public void onNext(S resource) {
			if (resourceProvided) {
				Operators.onNextDropped(resource, actual.currentContext());
				return;
			}
			resourceProvided = true;

			final Publisher<? extends T> p = deriveFluxFromResource(resource, resourceClosure);
			this.closureSubscriber = prepareSubscriberForResource(resource,
					this.actual,
					this.asyncComplete,
					this.asyncError,
					this.asyncCancel,
					this);

			p.subscribe(closureSubscriber);

			if (!isMonoSource) {
				resourceSubscription.cancel();
			}
		}

		@Override
		public Context currentContext() {
			return actual.currentContext();
		}

		@Override
		public void onError(Throwable throwable) {
			if (resourceProvided) {
				Operators.onErrorDropped(throwable, actual.currentContext());
				return;
			}
			//even if no resource provided, actual.onSubscribe has been called
			//let's immediately complete actual
			actual.onError(throwable);
		}

		@Override
		public void onComplete() {
			if (resourceProvided) {
				return;
			}
			//even if no resource provided, actual.onSubscribe has been called
			//let's immediately complete actual
			actual.onComplete();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.resourceSubscription, s)) {
				this.resourceSubscription = s;
				actual.onSubscribe(this);
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void cancel() {
			if (!resourceProvided) {
				resourceSubscription.cancel();
				super.cancel();
			}
			else {
				Operators.terminate(S, this);
				if (closureSubscriber != null) {
					closureSubscriber.cancel();
				}
			}
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return resourceSubscription;
			if (key == Attr.ACTUAL) return actual;
			if (key == Attr.PREFETCH) return Integer.MAX_VALUE;
			if (key == Attr.TERMINATED) return resourceProvided;

			return null;
		}

	}

	static class UsingWhenSubscriber<T, S> implements UsingWhenParent<T> {

		//state that differs in the different variants
		final CoreSubscriber<? super T>                                            actual;

		volatile Subscription                                                      s;
		static final AtomicReferenceFieldUpdater<UsingWhenSubscriber, Subscription>SUBSCRIPTION =
				AtomicReferenceFieldUpdater.newUpdater(UsingWhenSubscriber.class,
						Subscription.class, "s");

		//rest of the state is always the same
		final S                                                                resource;
		final Function<? super S, ? extends Publisher<?>>                      asyncComplete;
		final BiFunction<? super S, ? super Throwable, ? extends Publisher<?>> asyncError;
		@Nullable
		final Function<? super S, ? extends Publisher<?>>                      asyncCancel;
		@Nullable
		final DeferredSubscription                                             arbiter;

		volatile int callbackApplied;
		static final AtomicIntegerFieldUpdater<UsingWhenSubscriber> CALLBACK_APPLIED = AtomicIntegerFieldUpdater.newUpdater(UsingWhenSubscriber.class, "callbackApplied");

		/**
		 * Also stores the onComplete terminal state as {@link Exceptions#TERMINATED}
		 */
		Throwable error;

		UsingWhenSubscriber(CoreSubscriber<? super T> actual,
				S resource,
				Function<? super S, ? extends Publisher<?>> asyncComplete,
				BiFunction<? super S, ? super Throwable, ? extends Publisher<?>> asyncError,
				@Nullable Function<? super S, ? extends Publisher<?>> asyncCancel,
				@Nullable DeferredSubscription arbiter) {
			this.actual = actual;
			this.resource = resource;
			this.asyncComplete = asyncComplete;
			this.asyncError = asyncError;
			this.asyncCancel = asyncCancel;
			this.arbiter = arbiter;
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return this.actual;
		}

		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return error != null;
			if (key == Attr.ERROR) return (error == Exceptions.TERMINATED) ? null : error;
			if (key == Attr.CANCELLED) return s == Operators.cancelledSubscription();
			if (key == Attr.PARENT) return s;

			return UsingWhenParent.super.scanUnsafe(key);
		}

		@Override
		public void request(long l) {
			if (Operators.validate(l)) {
				s.request(l);
			}
		}

		@Override
		public void cancel() {
			if (CALLBACK_APPLIED.compareAndSet(this, 0, 1)) {
				if (Operators.terminate(SUBSCRIPTION, this)) {
					try {
						if (asyncCancel != null) {
							Flux.from(asyncCancel.apply(resource))
							    .subscribe(new CancelInner(this));
						}
						else {
							Flux.from(asyncComplete.apply(resource))
							    .subscribe(new CancelInner(this));
						}
					}
					catch (Throwable error) {
						Loggers.getLogger(FluxUsingWhen.class).warn("Error generating async resource cleanup during onCancel", error);
					}
				}
			}
		}

		@Override
		public void onNext(T t) {
			actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			if (CALLBACK_APPLIED.compareAndSet(this, 0, 1)) {
				Publisher<?> p;

				try {
					p = Objects.requireNonNull(asyncError.apply(resource, t),
							"The asyncError returned a null Publisher");
				}
				catch (Throwable e) {
					Throwable _e = Operators.onOperatorError(e, actual.currentContext());
					_e = Exceptions.addSuppressed(_e, t);
					actual.onError(_e);
					return;
				}

				p.subscribe(new RollbackInner(this, t));
			}
		}

		@Override
		public void onComplete() {
			if (CALLBACK_APPLIED.compareAndSet(this, 0, 1)) {
				Publisher<?> p;

				try {
					p = Objects.requireNonNull(asyncComplete.apply(resource),
							"The asyncComplete returned a null Publisher");
				}
				catch (Throwable e) {
					Throwable _e = Operators.onOperatorError(e, actual.currentContext());
					//give a chance for the Mono implementation to discard the recorded value
					deferredError(_e);
					return;
				}

				p.subscribe(new CommitInner(this));
			}
		}


		@Override
		public void deferredComplete() {
			this.error = Exceptions.TERMINATED;
			this.actual.onComplete();
		}

		@Override
		public void deferredError(Throwable error) {
			this.error = error;
			this.actual.onError(error);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				if (arbiter == null) {
					actual.onSubscribe(this);
				}
				else {
					arbiter.set(s);
				}
			}
		}
	}

	static final class UsingWhenConditionalSubscriber<T, S>
			extends UsingWhenSubscriber<T, S>
			implements ConditionalSubscriber<T> {

		final ConditionalSubscriber<? super T> actual;

		UsingWhenConditionalSubscriber(ConditionalSubscriber<? super T> actual,
				S resource,
				Function<? super S, ? extends Publisher<?>> asyncComplete,
				BiFunction<? super S, ? super Throwable, ? extends Publisher<?>> asyncError,
				@Nullable Function<? super S, ? extends Publisher<?>> asyncCancel,
				@Nullable DeferredSubscription arbiter) {
			super(actual, resource, asyncComplete, asyncError, asyncCancel, arbiter);
			this.actual = actual;
		}

		@Override
		public boolean tryOnNext(T t) {
			return actual.tryOnNext(t);
		}
	}

	static final class RollbackInner implements InnerConsumer<Object> {

		final UsingWhenParent parent;
		final Throwable       rollbackCause;

		boolean done;

		RollbackInner(UsingWhenParent ts, Throwable rollbackCause) {
			this.parent = ts;
			this.rollbackCause = rollbackCause;
		}

		@Override
		public Context currentContext() {
			return parent.currentContext();
		}

		@Override
		public void onSubscribe(Subscription s) {
			Objects.requireNonNull(s, "Subscription cannot be null")
			       .request(Long.MAX_VALUE);
		}

		@Override
		public void onNext(Object o) {
			//NO-OP
		}

		@Override
		public void onError(Throwable e) {
			done = true;
			RuntimeException rollbackError = new RuntimeException("Async resource cleanup failed after onError", e);
			parent.deferredError(Exceptions.addSuppressed(rollbackError, rollbackCause));
		}

		@Override
		public void onComplete() {
			done = true;
			parent.deferredError(rollbackCause);
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return parent;
			if (key == Attr.ACTUAL) return parent.actual();
			if (key == Attr.ERROR) return rollbackCause;
			if (key == Attr.TERMINATED) return done;

			return null;
		}
	}

	static final class CommitInner implements InnerConsumer<Object> {

		final UsingWhenParent parent;

		boolean done;

		CommitInner(UsingWhenParent ts) {
			this.parent = ts;
		}

		@Override
		public Context currentContext() {
			return parent.currentContext();
		}

		@Override
		public void onSubscribe(Subscription s) {
			Objects.requireNonNull(s, "Subscription cannot be null")
			       .request(Long.MAX_VALUE);
		}

		@Override
		public void onNext(Object o) {
			//NO-OP
		}

		@Override
		public void onError(Throwable e) {
			done = true;
			Throwable e_ = Operators.onOperatorError(e, parent.currentContext());
			Throwable commitError = new RuntimeException("Async resource cleanup failed after onComplete", e_);
			parent.deferredError(commitError);
		}

		@Override
		public void onComplete() {
			done = true;
			parent.deferredComplete();
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return parent;
			if (key == Attr.ACTUAL) return parent.actual();
			if (key == Attr.TERMINATED) return done;

			return null;
		}
	}

	/**
	 * Used	in the cancel path to still give the generated Publisher access to the Context
 	 */
	static final class CancelInner implements InnerConsumer<Object> {

		final UsingWhenParent parent;

		CancelInner(UsingWhenParent ts) {
			this.parent = ts;
		}

		@Override
		public Context currentContext() {
			return parent.currentContext();
		}

		@Override
		public void onSubscribe(Subscription s) {
			Objects.requireNonNull(s, "Subscription cannot be null")
			       .request(Long.MAX_VALUE);
		}

		@Override
		public void onNext(Object o) {
			//NO-OP
		}

		@Override
		public void onError(Throwable e) {
			Loggers.getLogger(FluxUsingWhen.class).warn("Async resource cleanup failed after cancel", e);
		}

		@Override
		public void onComplete() {
			//NO-OP
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return parent;
			if (key == Attr.ACTUAL) return parent.actual();

			return null;
		}
	}

	private interface UsingWhenParent<T> extends InnerOperator<T, T> {

		void deferredComplete();

		void deferredError(Throwable error);
	}
}
