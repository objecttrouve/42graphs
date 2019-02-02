/*
 * MIT License
 *
 * Copyright (c) 2019 objecttrouve.org <un.object.trouve@gmail.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.objecttrouve.fourtytwo.graphs.procedures.aggregating;

import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class PositionCountsFromLengthsTest {

    @Test
    public void test__add__length_0__and_get(){
        final PositionCountsFromLengths positionCountsFromLengths = new PositionCountsFromLengths();

        positionCountsFromLengths.add(0);
        final int[] positionCounts = positionCountsFromLengths.get();

        assertThat(positionCounts.length, is(0));
    }

    @Test
    public void test__add__length_1__and_get(){
        final PositionCountsFromLengths positionCountsFromLengths = new PositionCountsFromLengths();

        positionCountsFromLengths.add(1);
        final int[] positionCounts = positionCountsFromLengths.get();

        assertThat(positionCounts.length, is(1));
        assertThat(positionCounts[0], is(1));
    }

    @Test
    public void test__add__length_5__and_get(){
        final PositionCountsFromLengths positionCountsFromLengths = new PositionCountsFromLengths();

        positionCountsFromLengths.add(5);
        final int[] positionCounts = positionCountsFromLengths.get();

        assertThat(positionCounts.length, is(5));
        assertThat(positionCounts[0], is(1));
        assertThat(positionCounts[1], is(1));
        assertThat(positionCounts[2], is(1));
        assertThat(positionCounts[3], is(1));
        assertThat(positionCounts[4], is(1));

    }

    @Test
    public void test__add__length_4_5_2__and_get(){
        final PositionCountsFromLengths positionCountsFromLengths = new PositionCountsFromLengths();

        positionCountsFromLengths.add(4);
        positionCountsFromLengths.add(5);
        positionCountsFromLengths.add(2);
        final int[] positionCounts = positionCountsFromLengths.get();

        assertThat(positionCounts.length, is(5));
        assertThat(positionCounts[0], is(3));
        assertThat(positionCounts[1], is(3));
        assertThat(positionCounts[2], is(2));
        assertThat(positionCounts[3], is(2));
        assertThat(positionCounts[4], is(1));
    }

}