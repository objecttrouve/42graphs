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

import java.util.Arrays;

import static java.util.Arrays.copyOf;

class PositionCountsFromLengths {

    private static final int randomDefaultLength = 20;

    private int[] positionCounts = new int[randomDefaultLength];

    void add(final int length){
        if (positionCounts.length < length){
            positionCounts = copyOf(positionCounts, length*2);
        }
        for (int i = 0; i < length ; i++) {
            positionCounts[i]++;
        }
    }

    int[] get(){
        for (int i = 0; i < positionCounts.length; i++) {
            if(positionCounts[i] == 0){
                return copyOf(positionCounts, i);
            }
        }
        return copyOf(positionCounts, positionCounts.length);
    }

    @Override
    public String toString() {
        return "PositionCountsFromLengths{" +
            "positionCounts=" + Arrays.toString(positionCounts) +
            '}';
    }
}
