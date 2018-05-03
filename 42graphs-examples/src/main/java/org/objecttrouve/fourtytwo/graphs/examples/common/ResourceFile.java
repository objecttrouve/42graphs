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

package org.objecttrouve.fourtytwo.graphs.examples.common;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class ResourceFile {

    private final String name;

    public static ResourceFile file(final String name){
        return new ResourceFile(name);
    }
    private ResourceFile(final String name) {
        this.name = name;
    }

    InputStream inputStream(){
        return Optional.ofNullable(this.getClass().getClassLoader().getResourceAsStream(name))
            .or(() -> {
                final Path localPath = Paths.get("src/main/resources").resolve(name);
                try {
                    return Optional.of(Files.newInputStream(localPath));
                } catch (IOException e) {
                    throw new IllegalArgumentException("Could not load '" + localPath + "'.");
                }
            })
            .orElseThrow(()-> new IllegalArgumentException("Could not load '" + name + "'."));
    }

    public String read(){
        try {
            return CharStreams.toString(new InputStreamReader(
                  inputStream(), Charsets.UTF_8));
        } catch (final IOException e) {
            throw new RuntimeException("Could not read resource file '" + name + "'.", e);
        }

    }
}
