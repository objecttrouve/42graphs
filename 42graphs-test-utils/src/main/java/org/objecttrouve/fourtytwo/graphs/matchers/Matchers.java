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

package org.objecttrouve.fourtytwo.graphs.matchers;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.objecttrouve.testing.matchers.ConvenientMatchers;
import org.objecttrouve.testing.matchers.api.Stringifiers;
import org.objecttrouve.testing.matchers.customization.MatcherFactory;
import org.objecttrouve.testing.matchers.customization.StringifiersConfig;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.objecttrouve.fourtytwo.graphs.api.Value.idKey;

class Matchers {


    private static final Stringifiers stringifiers = StringifiersConfig.stringifiers()
        .withShortStringifier(Node.class, Matchers::printNodeShort)
        .withDebugStringifier(Node.class, Matchers::printNodeDebug)
        .build();

    static final MatcherFactory an = ConvenientMatchers.customized()
        .withStringifiers(stringifiers)
        .debugging()
        .build();

    private static String printNodeShort(final Node n) {
        return StreamSupport.stream(n.getLabels().spliterator(), true)
            .map(Label::name)
            .collect(joining(",")) + ":" + n.getProperty(idKey, "");
    }

    private static String printNodeDebug(final Node n) {


        final String self = printNodeShort(n) + " ["
            + getKeys(n)
            .stream()
            .map(k -> k + "=" + getProp(n, k)).collect(joining(",")) +
            "]";
        final String linked = getOutgoing(n).stream().map(next -> "⤷" + next).collect(joining("\n\t"));
        return isNotBlank(linked) ? self + "\n\t" + linked : self;
    }

    private static Object getProp(final Node n, final String k) {
        return n.getProperty(k, null);
    }

    private static List<String> getKeys(final Node n) {
        /*
        * Fetching values while iterating keys directly causes weird exceptions.
        * So, collect first.
        */
        return StreamSupport.stream(n.getPropertyKeys().spliterator(), true).collect(Collectors.toList());
    }

    private static List<String> getOutgoing(final Node n) {

        return StreamSupport.stream(n.getRelationships(Direction.OUTGOING).spliterator(), true)
            // Intentional.
            .collect(Collectors.toList())
            .stream()
            .map(r -> r.getEndNode().getProperty(idKey))
            .map(Object::toString)
            .collect(Collectors.toList());
    }
}
