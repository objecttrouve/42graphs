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

package org.objecttrouve.fourtytwo.graphs.examples.common;

import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;

import java.io.IOException;
import java.io.InputStream;

public class SentenceDetector {

    private final SentenceDetectorME detector;

    private SentenceDetector(final SentenceDetectorME detector) {

        this.detector = detector;
    }

    public static SentenceDetector load(final String modelFile){
        //Loading sentence detector modelFile
        final InputStream inputStream = ResourceFile.file(modelFile).inputStream();
        final SentenceModel model;
        try {
            model = new SentenceModel(inputStream);
        } catch (final IOException e) {
            throw new RuntimeException("Could not load sentence detector modelFile.", e);
        }
        //Instantiating the SentenceDetectorME class
        final SentenceDetectorME detector = new SentenceDetectorME(model);
        return new SentenceDetector(detector);
    }

    public String[] process(final String text){
        return detector.sentDetect(text);
    }
}
