/*
 * Copyright (c) 2011-2019 Pivotal Software Inc, All Rights Reserved.
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

package reactor.core;

import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Hooks;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * A {@link CoreSubscriber} aware publisher that can also directly decorate subscribers instead of recursively subscribing.
 *
 *
 * @param <T> the {@link CoreSubscriber} data type
 *
 * @since 3.3.0
 */
public interface CorePublisher<T> extends Publisher<T> {

	/**
	 * Will return a parent {@link CorePublisher} if available or "null". When "null" is returned, it usually signals the start of a reactor operator chain.
	 *
	 * @return any parent {@link CorePublisher} if any or "null" if the source is not a {@link CorePublisher} or there is no further parent
	 */
	@Nullable
	default CorePublisher<?> source() {
		return null;
	}

	/**
	 * An internal {@link Publisher#subscribe(Subscriber)} that will bypass
	 * {@link Hooks#onLastOperator(Function)} pointcut.
	 * <p>
	 * In addition to behave as expected by {@link Publisher#subscribe(Subscriber)}
	 * in a controlled manner, it supports direct subscribe-time {@link Context} passing.
	 *
	 * @param subscriber the {@link Subscriber} interested into the published sequence
	 * @see Publisher#subscribe(Subscriber)
	 */
	void subscribe(CoreSubscriber<? super T> subscriber);

	/**
	 * Return next {@link CoreSubscriber} or "null" if the subscription was already done inside the method
	 * @return next {@link CoreSubscriber} or "null" if the subscription was already done inside the method
	 */
	@Nullable
	default CoreSubscriber<?> subscribeOrReturn(CoreSubscriber<? super T> subscriber) {
		return subscriber;
	}
}
