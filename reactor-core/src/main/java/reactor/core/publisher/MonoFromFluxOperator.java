/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.Objects;

import org.reactivestreams.Publisher;
import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;

/**
 * A decorating {@link Mono} {@link Publisher} that exposes {@link Mono} API over an
 * arbitrary {@link Publisher} Useful to create operators which return a {@link Mono}.
 *
 * @param <I> delegate {@link Publisher} type
 * @param <O> produced type
 */
abstract class MonoFromFluxOperator<I, O> extends Mono<O> implements Scannable {

	protected final Flux<? extends I> source;

	/**
	 * Build a {@link MonoFromFluxOperator} wrapper around the passed parent {@link Publisher}
	 *
	 * @param source the {@link Publisher} to decorate
	 */
	protected MonoFromFluxOperator(Flux<? extends I> source) {
		this.source = Objects.requireNonNull(source);
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PREFETCH) return Integer.MAX_VALUE;
		if (key == Attr.PARENT) return source;
		return null;
	}

	@Override
	@SuppressWarnings("unchecked")
	public final void subscribe(CoreSubscriber<? super O> subscriber) {
		CorePublisher publisher = this;
		CorePublisher next = publisher;
		CoreSubscriber liftedSubscriber;
		for(;;) {
			liftedSubscriber = next.subscribeOrReturn(subscriber);

			if (liftedSubscriber == null) {
				return;
			}

			publisher = next;
			next = publisher.source();

			if (next == null) {
				publisher.subscribe(subscriber);
				return;
			}
			subscriber = liftedSubscriber;
		}
	}

	@Override
	public final CorePublisher<? extends I> source() {
		return source;
	}

}
