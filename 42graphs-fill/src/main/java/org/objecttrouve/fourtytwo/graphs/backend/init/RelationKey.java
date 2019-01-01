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

package org.objecttrouve.fourtytwo.graphs.backend.init;

public class RelationKey {
  private final long childNode;
  private final long parentNode;
  private final String dimension;
  private final int position;


  public static RelationKey key(final long childNode, final long parentNode, final String dimension, final int position){
    return new RelationKey(childNode, parentNode, dimension, position);
  }

  public RelationKey(final long childNode, final long parentNode, final String dimension, final int position) {
    this.childNode = childNode;
    this.parentNode = parentNode;
    this.dimension = dimension;
    this.position = position;
  }

  public long getChildNode() {
    return childNode;
  }

  public long getParentNode() {
    return parentNode;
  }

  public String getDimension() {
    return dimension;
  }

  public int getPosition() {
    return position;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    final RelationKey that = (RelationKey) o;

    if (childNode != that.childNode) return false;
    if (parentNode != that.parentNode) return false;
    if (position != that.position) return false;
    return dimension != null ? dimension.equals(that.dimension) : that.dimension == null;
  }

  @Override
  public int hashCode() {
    int result = (int) (childNode ^ (childNode >>> 32));
    result = 31 * result + (int) (parentNode ^ (parentNode >>> 32));
    result = 31 * result + (dimension != null ? dimension.hashCode() : 0);
    result = 31 * result + position;
    return result;
  }



}
