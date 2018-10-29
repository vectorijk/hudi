/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie;

import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Default payload used for delta streamer.
 * <p>
 * 1. preCombine - Picks the latest delta record for a key, based on an ordering field 2.
 * combineAndGetUpdateValue/getInsertValue - Simply overwrites storage with latest delta record
 */
public class HistoryAvroPayload extends BaseAvroPayload implements
    HoodieRecordPayload<HistoryAvroPayload> {
//  public List<?> history = Collections.singletonList(-1);

  /**
   * @param record
   * @param orderingVal
   */
  public HistoryAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public HistoryAvroPayload(Optional<GenericRecord> record) {
    this(record.get(), (record1) -> 0); // natural order
  }

  @Override
//  public HistoryAvroPayload preCombine(HistoryAvroPayload another) {
//    return this;
//  }

  public HistoryAvroPayload preCombine(HistoryAvroPayload another) {
    // pick the payload with greatest ordering value
    if (another.orderingVal.compareTo(orderingVal) > 0) {
      Long l1 = (Long) another.record.get("location");
      Long l2 = (Long) this.record.get("location");

      if (another.record.get("history") instanceof GenericArray) {
        ((GenericArray) another.record.get("history")).add(l2);
//        ((GenericArray) another.record.get("history")).add(l1);
      } if (another.record.get("history") instanceof ArrayList) {
        ((ArrayList) another.record.get("history")).add(l2);
//        ((ArrayList) another.record.get("history")).add(l1);
      }

      //      another.history = Lists.newArrayList(Iterables.concat(another.history, this.history));
      return another;
    } else {
      Long l1 = (Long) another.record.get("location");
      Long l2 = (Long) this.record.get("location");

      //      ((GenericArray) this.record.get("history")).add(l2);
      //      ((GenericArray) this.record.get("history")).add(l1);

      if (this.record.get("history") instanceof GenericArray) {
//        ((GenericArray) this.record.get("history")).add(l2);
        ((GenericArray) this.record.get("history")).add(l1);
      } if (this.record.get("history") instanceof ArrayList) {
//        ((ArrayList) this.record.get("history")).add(l2);
        ((ArrayList) this.record.get("history")).add(l1);
      }

      //      history = Lists.newArrayList(Iterables.concat(another.history, this.history));
      return this;
    }
  }

  @Override
  public Optional<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) {
    Long location2 = (Long) ((GenericRecord) currentValue).get("location");
    Long location1 = (Long) record.get("location");

    List<?> array1 = ((ArrayList) ((GenericRecord) currentValue).get("history"));
    List<?> array2 = ((GenericArray) record.get("history"));

    ((GenericData.Array) record.get("history")).add(location1);
    ((GenericData.Array) record.get("history")).add(location2);
    for (Object N: array1) {
      ((GenericData.Array) record.get("history")).add((Long) N);
    }

    return Optional.of(HoodieAvroUtils.rewriteRecord(record, schema));
  }

  @Override
  public Optional<IndexedRecord> getInsertValue(Schema schema) {
    return Optional.of(HoodieAvroUtils.rewriteRecord(record, schema));
  }
}
