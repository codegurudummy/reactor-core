/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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
package reactor.util;

import org.junit.Test;
import reactor.core.Exceptions;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephane Maldini
 */
public class ExceptionTests {

	@Test
	public void testWrapping() throws Exception {

		Throwable t = new Exception("test");

		Throwable w = Exceptions.bubble(Exceptions.propagate(t));

		assertTrue(Exceptions.unwrap(w) == t);
	}

	@Test
	public void allOverflowIsIllegalState() {
		IllegalStateException overflow1 = Exceptions.failWithOverflow();
		IllegalStateException overflow2 = Exceptions.failWithOverflow("foo");

		assertTrue(Exceptions.isOverflow(overflow1));
		assertTrue(Exceptions.isOverflow(overflow2));
	}

	@Test
	public void allIllegalStateIsntOverflow() {
		IllegalStateException ise = new IllegalStateException("foo");

		assertFalse(Exceptions.isOverflow(ise));
	}

}