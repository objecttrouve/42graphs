/*
 * MIT License
 *
 * Copyright (c) 2018 objecttrouve.org <un.object.trouve@gmail.com>
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

package org.objecttrouve.fourtytwo.graphs.examples;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.objecttrouve.fourtytwo.graphs.categories.Snore;
import org.objecttrouve.fourtytwo.graphs.examples.x000.warmup.WarmUpMain;
import org.objecttrouve.fourtytwo.graphs.examples.x001.count.CountStuffMain;
import org.objecttrouve.fourtytwo.graphs.examples.x002.retrieve.RetrieveStuffMain;
import org.objecttrouve.fourtytwo.graphs.examples.x003.retrieve.aggregated.RetrieveAggregatedStuffMain;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Consumer;

@Category(Snore.class)
public class ExamplesSanityTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @After
    public void cleanup(){
        tmpFolder.delete();
    }

    @Test
    public void run_all_examples() throws IOException {
        run(WarmUpMain::main);
        run(CountStuffMain::main);
        run(RetrieveStuffMain::main);
        run(RetrieveAggregatedStuffMain::main);
    }

    private void run(final Consumer<String[]> main) throws IOException {
        final Path db = tmpFolder.newFolder().toPath();
        final String[] args = {"-o", db.toAbsolutePath().toString(), "-c"};
        main.accept(args);
    }
}