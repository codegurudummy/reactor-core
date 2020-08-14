/*
 * Copyright (c) 2011-2019 Pivotal Software Inc, All Rights Reserved.
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

package reactor.test;

import java.util.function.Function;
import java.util.function.Supplier;

import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * @author Simon Baslé
 */
public interface StepVerifierOptionsBuilder<SELF extends StepVerifierOptionsBuilder> {

	/**
	 * Activate or deactivate the {@link StepVerifier} check of request amount
	 * being too low. Defauts to true.
	 *
	 * @param enabled true if the check should be enabled.
	 * @return this instance, to continue setting the options.
	 */
	SELF checkUnderRequesting(boolean enabled);

	/**
	 * Set the amount the {@link StepVerifier} should request initially. Defaults to
	 * unbounded request ({@code Long.MAX_VALUE}).
	 *
	 * @param initialRequest the initial request amount.
	 * @return this instance, to continue setting the options.
	 */
	SELF initialRequest(long initialRequest);

	/**
	 * Set up a custom value formatter to be used in error messages when presenting
	 * expected and actual values. This is intended for classes that have obscure {@link #toString()}
	 * implementation that cannot be overridden.
	 * <p>
	 * This is a {@link Function} capable of formatting an arbitrary {@link Object} to
	 * {@link String}, with the intention of detecting elements from the sequence under
	 * test and applying customized {@link String} conversion to them (and simply calling
	 * {@link #toString()} on other objects).
	 * <p>
	 * See {@link ValueFormatters} for factories of such functions.
	 *
	 * @param valueFormatter the custom value to {@link String} formatter, or null to deactivate
	 * custom formatting
	 * @return this instance, to continue setting the options
	 */
	SELF valueFormatter(@Nullable ValueFormatters.ToStringConverter valueFormatter);

	/**
	 * Add an {@link ValueFormatters.Extractor}, replacing any existing {@link ValueFormatters.Extractor} that targets the
	 * same {@link Class} (as in {@link ValueFormatters.Extractor#getTargetClass()}).
	 * <p>
	 * Note that by default, default extractors for {@link ValueFormatters#signalExtractor() Signal},
	 * {@link ValueFormatters#iterableExtractor() Iterable} and
	 * {@link ValueFormatters#arrayExtractor(Class) Object[]} are in place.
	 *
	 *
	 * @param extractor the extractor to add / set
	 * @param <T> the type of container considered by this extractor
	 * @return this instance, to continue setting the options
	 */
	<T> SELF extractor(ValueFormatters.Extractor<T> extractor);

	/**
	 * Set a supplier for a {@link VirtualTimeScheduler}, which is mandatory for a
	 * {@link StepVerifier} to work with virtual time. Defaults to null.
	 *
	 * @param vtsLookup the supplier of {@link VirtualTimeScheduler} to use.
	 * @return this instance, to continue setting the options.
	 */
	SELF virtualTimeSchedulerSupplier(Supplier<? extends VirtualTimeScheduler> vtsLookup);

	/**
	 * Set an initial {@link Context} to be propagated by the {@link StepVerifier} when it
	 * subscribes to the sequence under test.
	 *
	 * @param context the {@link Context} to propagate.
	 * @return this instance, to continue setting the options.
	 */
	SELF withInitialContext(Context context);

	/**
	 * Give a name to the whole scenario tested by the configured {@link StepVerifier}. That
	 * name would be mentioned in exceptions and assertion errors raised by the StepVerifier,
	 * allowing to better distinguish error sources in unit tests where multiple StepVerifier
	 * are used.
	 *
	 * @param scenarioName the name of the scenario, null to deactivate
	 * @return this instance, to continue setting the options.
	 */
	SELF scenarioName(@Nullable String scenarioName);
}
