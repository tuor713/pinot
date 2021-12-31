/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.spi.index;

import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Helper wrapper class for {@link MutableRoaringBitmap} to make it thread-safe.
 */
public class ThreadSafeMutableRoaringBitmap {
  private final MutableRoaringBitmap _mutableRoaringBitmap;

  /**
   * Object to lock on - can be this for use cases outside validDocIds. For validDocIds it's the PartitionUpsertMetadataManager
   * object, which is the same for all bitmaps in the partition.
   */
  private final Object _lock;

  public ThreadSafeMutableRoaringBitmap() {
    _mutableRoaringBitmap = new MutableRoaringBitmap();
    _lock = this;
  }

  public ThreadSafeMutableRoaringBitmap(Object lock) {
    _mutableRoaringBitmap = new MutableRoaringBitmap();
    _lock = lock;
  }

  public ThreadSafeMutableRoaringBitmap(int firstDocId) {
    _mutableRoaringBitmap = new MutableRoaringBitmap();
    _mutableRoaringBitmap.add(firstDocId);
    _lock = this;
  }

  public ThreadSafeMutableRoaringBitmap(MutableRoaringBitmap bitmap) {
    _mutableRoaringBitmap = bitmap;
    _lock = this;
  }

  public void add(int docId) {
    synchronized (_lock) {
      _mutableRoaringBitmap.add(docId);
    }
  }

  public boolean contains(int docId) {
    synchronized (_lock) {
      return _mutableRoaringBitmap.contains(docId);
    }
  }

  public void remove(int docId) {
    synchronized (_lock) {
      _mutableRoaringBitmap.remove(docId);
    }
  }

  public void replace(int oldDocId, int newDocId) {
    synchronized (_lock) {
      _mutableRoaringBitmap.remove(oldDocId);
      _mutableRoaringBitmap.add(newDocId);
    }
  }

  public void replace(ThreadSafeMutableRoaringBitmap oldBitmap, int oldDocId, int newDocId) {
    synchronized (_lock) {
      oldBitmap.remove(oldDocId);
      _mutableRoaringBitmap.add(newDocId);
    }
  }

  public void replace(ThreadSafeMutableRoaringBitmap oldBitmap, int oldDocId, ThreadSafeMutableRoaringBitmap oldConcurrentBitmap, int oldConcurrentDocId, int newDocId) {
    synchronized (_lock) {
      oldBitmap.remove(oldDocId);
      if (oldConcurrentBitmap != null) {
        oldConcurrentBitmap.remove(oldConcurrentDocId);
      }
      _mutableRoaringBitmap.add(newDocId);
    }
  }

  public MutableRoaringBitmap getMutableRoaringBitmap() {
    synchronized (_lock) {
      return _mutableRoaringBitmap.clone();
    }
  }

  public Object getLock() {
    return _lock;
  }
}
