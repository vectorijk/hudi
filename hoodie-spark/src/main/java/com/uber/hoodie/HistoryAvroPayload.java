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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.tuple.Pair;

/** History payload. */
public class HistoryAvroPayload extends BaseAvroPayload
    implements HoodieRecordPayload<HistoryAvroPayload> {
  private String ser(Long location, String ts) {
    return ts + "#" + location.toString();
  }

  private String serString(Pair<String, String> payload) {
    return payload.getLeft() + "#" + payload.getRight();
  }

  private Utf8 serUtf8(Pair<String, String> payload) {
    String tmp = payload.getLeft() + "#" + payload.getRight();
    return new Utf8(tmp);
  }

  private Pair<String, String> deSer(String payload) {
    // pair < timestamp, location >

    String[] tmp = payload.split("#");
    return Pair.of(tmp[0], tmp[1]);
  }

  private Pair<String, String> deSer(Utf8 payload) {
    // pair < timestamp, location >

    String[] tmp = payload.toString().split("#");
    return Pair.of(tmp[0], tmp[1]);
  }

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

  //  original
  public HistoryAvroPayload preCombine(HistoryAvroPayload another) {
    if (another.orderingVal.compareTo(orderingVal) > 0) {
      Long l1 = (Long) another.record.get("location");
      Long l2 = (Long) this.record.get("location");

      if (another.record.get("history") instanceof GenericArray) {
        ((GenericArray) another.record.get("history")).add(ser(l2, orderingVal.toString()));

        //        ((GenericArray) another.record.get("history")).add(ser(l1,
        // another.orderingVal.toString()));
      } else if (another.record.get("history") instanceof ArrayList) {
        // below
        ((ArrayList) another.record.get("history")).add(ser(l2, orderingVal.toString()));

        //        ((ArrayList) another.record.get("history")).add("+0" + ser(l2,
        // orderingVal.toString()));
        //        ((ArrayList) another.record.get("history")).add("+0" + ser(l1,
        // another.orderingVal.toString()));
      }

      //      another.history = Lists.newArrayList(Iterables.concat(another.history, this.history));
      return another;
    } else {
      Long l1 = (Long) another.record.get("location");
      Long l2 = (Long) this.record.get("location");

      //      ((GenericArray) this.record.get("history")).add(l2);
      //      ((GenericArray) this.record.get("history")).add(l1);

      if (this.record.get("history") instanceof GenericArray) {
        //        ((GenericArray) this.record.get("history")).add(ser(l2, orderingVal.toString()));

        ((GenericArray) this.record.get("history")).add(ser(l1, another.orderingVal.toString()));
      } else if (this.record.get("history") instanceof ArrayList) {
        //        ((ArrayList) this.record.get("history")).add("0" + ser(l2,
        // orderingVal.toString()));
        //        ((ArrayList) this.record.get("history")).add("+0" + ser(l1,
        // another.orderingVal.toString()));

        // below
        ((ArrayList) this.record.get("history")).add(ser(l1, another.orderingVal.toString()));
      }

      //      history = Lists.newArrayList(Iterables.concat(another.history, this.history));
      return this;
    }
  }

  //  public HistoryAvroPayload preCombine(HistoryAvroPayload another) {
  ////    if (another.orderingVal.compareTo(orderingVal) > 0) {
  //      Long l1 = (Long) another.record.get("location");
  //      Long l2 = (Long) this.record.get("location");
  //
  //      if (another.record.get("history") instanceof GenericArray) {
  //        ((GenericArray) another.record.get("history")).add(ser(l2, orderingVal.toString()));
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //        //        ((GenericArray) another.record.get("history")).add(ser(l1,
  // another.orderingVal.toString()));
  //      }
  //      else if (another.record.get("history") instanceof ArrayList) {
  //        //below
  //        ((ArrayList) another.record.get("history")).add(ser(l2, orderingVal.toString()));
  //
  //
  //
  //
  //        //        ((ArrayList) another.record.get("history")).add("+0" + ser(l2,
  // orderingVal.toString()));
  //        //        ((ArrayList) another.record.get("history")).add("+0" + ser(l1,
  // another.orderingVal.toString()));
  //      }
  //
  //      //      another.history = Lists.newArrayList(Iterables.concat(another.history,
  // this.history));
  //      return another;
  ////    } else {
  ////      Long l1 = (Long) another.record.get("location");
  ////      Long l2 = (Long) this.record.get("location");
  ////
  ////      //      ((GenericArray) this.record.get("history")).add(l2);
  ////      //      ((GenericArray) this.record.get("history")).add(l1);
  ////
  ////      if (this.record.get("history") instanceof GenericArray) {
  ////        //        ((GenericArray) this.record.get("history")).add(ser(l2,
  // orderingVal.toString()));
  ////
  ////
  ////
  ////
  ////
  ////
  ////        ((GenericArray) this.record.get("history")).add(ser(l1,
  // another.orderingVal.toString()));
  ////      }
  ////      else if (this.record.get("history") instanceof ArrayList) {
  ////        //        ((ArrayList) this.record.get("history")).add("0" + ser(l2,
  // orderingVal.toString()));
  ////        //        ((ArrayList) this.record.get("history")).add("0" + ser(l1,
  // another.orderingVal.toString()));
  ////
  ////
  ////
  ////
  ////        //below
  ////        ((ArrayList) this.record.get("history")).add(ser(l1, another.orderingVal.toString()));
  ////      }
  ////
  ////      //      history = Lists.newArrayList(Iterables.concat(another.history, this.history));
  ////      return this;
  ////    }
  //  }

  //  public List<String> mergeListElem(List<String> origin, String newElem) {
  //    origin.add(newElem);
  //
  //    return origin
  //        .stream()
  //        .map((elem) -> deSer(elem))
  //        .sorted(
  //            (cmp1, cmp2) ->
  // Double.valueOf(cmp2.getKey()).compareTo(Double.valueOf(cmp1.getKey())))
  //        .map((pair) -> serString(pair))
  //        .collect(Collectors.toList());
  //  }

  public List<Utf8> mergeListList(List<String> origin, List<String> newList) {
    //    origin.addAll(newList);

    List<String> tmp = new ArrayList<>();

    for (Object o : origin) {
      tmp.add(o.toString());
    }

    for (Object o : newList) {
      tmp.add(o.toString());
    }

    List<Utf8> result =
        tmp.stream()
            .map((elem) -> deSer(elem))
            .sorted(
                (cmp1, cmp2) ->
                    Double.valueOf(cmp2.getKey()).compareTo(Double.valueOf(cmp1.getKey())))
            .map((pair) -> serUtf8(pair))
            .collect(Collectors.toList());

    return result;
  }

  @Override
  public Optional<IndexedRecord> combineAndGetUpdateValue(
      IndexedRecord currentValue, Schema schema) {
    Long location1 = (Long) record.get("location");
    Long location2 = (Long) ((GenericRecord) currentValue).get("location");

    Double ts1 = (Double) record.get("timestamp");
    Double ts2 = (Double) ((GenericRecord) currentValue).get("timestamp");

    List<Utf8> array1 = ((ArrayList) ((GenericRecord) currentValue).get("history"));

    List<String> array11 = new ArrayList<>();

    for (Utf8 a : array1) {
      array11.add(a.toString());
    }

    GenericData.Array array2 = (GenericData.Array) record.get("history");
    List<String> array22 =
        (List) array2.stream().map((elem) -> ((Utf8) elem).toString()).collect(Collectors.toList());

    //    if ( array1.size() == 0 ) {
    //      array22.add(((GenericRecord) currentValue).get("timestamp") + "#" +
    // location2.toString());
    //      array22.add(((GenericRecord) record).get("timestamp") + "#" + location1.toString());
    //    }
    //    /************
    //     *
    //    if (array22.size() == 0 && array1.size() == 0) {
    //      array22.add(((GenericRecord) currentValue).get("timestamp") + "#" +
    // location2.toString());
    //    }

    //    ****************/
    //    if (array1.size() == 0 ) {
    //    /*
    //
    //    array11.add(record.get("timestamp") + "#" + location1.toString());
    //    */
    //      array22.add(((GenericRecord) currentValue).get("timestamp") + "#" +
    // location2.toString());
    //    }
    //    if ( array2.size() == 0 ) {
    //      array1.add(location1.toString());
    //    }

    //    for (Object o: array2) {
    //      System.out.println(o);
    //    }

    List<Utf8> resultArray = mergeListList(array11, array22);
    //    List<?> array2 = ((GenericArray) record.get("history"));

    //    ((GenericData.Array) record.get("history")).add(location1);

    //    if (array1.size() == 0) {
    //      ((GenericData.Array) record.get("history")).add(((GenericRecord)
    // currentValue).get("timestamp") + "#" + location2.toString());
    //    }
    //
    //    for (Object N : array1) {
    //      ((GenericData.Array) record.get("history")).add(N.toString());
    //    }
    //
    record.put("history", resultArray);
    return Optional.of(HoodieAvroUtils.rewriteRecord(record, schema));
  }

  @Override
  public Optional<IndexedRecord> getInsertValue(Schema schema) {
    Long location1 = (Long) record.get("location");
    Double ts1 = (Double) record.get("timestamp");
    //    if (((GenericData.Array) record.get("history")).size() == 0) {
    //      ((GenericData.Array) record.get("history")).add("9999999" + ts1.toString() + "#" +
    // location1.toString());
    ((GenericData.Array) record.get("history")).add(ts1.toString() + "#" + location1.toString());
    return Optional.of(HoodieAvroUtils.rewriteRecord(record, schema));
  }

  //  @Override
  //  public Optional<IndexedRecord> getInsertValue(Schema schema) {
  //    Long location1 = (Long) record.get("location");
  //    Double ts1 = (Double) record.get("timestamp");
  //    if (((GenericData.Array) record.get("history")).size() <= 1) {
  //      ((GenericData.Array) record.get("history")).add("+" + ts1.toString() + "#" +
  // location1.toString());
  ////      ((GenericData.Array) record.get("history")).add(ts1.toString() + "#" +
  // location1.toString());
  //    }
  //    return Optional.of(HoodieAvroUtils.rewriteRecord(record, schema));
  //  }
}
