package brickhouse.udf.sketch;
/**
 * Copyright 2012 Klout, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **/



import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
import gnu.trove.set.hash.TLongHashSet;


/**
 *  Construct a sketch set by aggregating over a a set of ID's
 *  
 *
 */

@Description(name="sketch_set_cnt",
    value = "_FUNC_(x) - Constructs a sketch set and estimates reach for large values  "
)
public class SketchSetCntUDAF extends AbstractGenericUDAFResolver {
  private static final Logger LOG = Logger.getLogger(SketchSetCntUDAF.class);
  public static int DEFAULT_SKETCH_SET_SIZE =  5000;
  static String SKETCH_SIZE_STR = "SKETCH_SIZE";


  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
          throws SemanticException {

      if( !parameters[0].getTypeName().equals("string")
       && !parameters[0].getTypeName().equals("bigint")) {
          throw new SemanticException("sketch_set_cnt UDAF only takes String or longs as values; not " + parameters[0].getTypeName());
      }
      if((parameters.length > 1) && !parameters[1].getTypeName().equals("int")) {
          throw new SemanticException("Size of sketch must be an int; Got " + parameters[1].getTypeName());
      }
      SketchSetUDAFEvaluator se = new SketchSetUDAFEvaluator();
      se.setSketchSetType(parameters[0].getTypeName().equals("bigint") ? 1 : 0);
      //return new SketchSetUDAFEvaluator();//parameters[0].getTypeName().equals("bigint") ? 1 : 0);
      return se;
  }


  public static class SketchSetUDAFEvaluator extends GenericUDAFEvaluator {
    // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
	  private StringObjectInspector inputStrOI;
      private PrimitiveObjectInspector inputPrimitiveOI;
      private StandardListObjectInspector partialOI;
	  private int sketchSetSize = -1;

      public int getSketchSetType() {
          return sketchSetType;
      }

      public void setSketchSetType(int sketchSetType) {
          this.sketchSetType = sketchSetType;
      }

      private int sketchSetType = 0;

    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
        throws HiveException {
      super.init(m, parameters);
      /// 
      if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
          ObjectInspector.Category cat = parameters[0].getCategory();
          switch (cat) {
              case PRIMITIVE:
                  break;
              default:
                  throw new IllegalArgumentException(
                          "Only PRIMITIVE types are allowed as input. Passed a "
                                  + cat.name());
          }

          if(sketchSetType == 0) {
              this.inputStrOI = (StringObjectInspector) parameters[0];
          } else {
              this.inputPrimitiveOI = (PrimitiveObjectInspector) parameters[0];
          }


    	  if( parameters.length > 1 && m == Mode.PARTIAL1) {
    	     //// get the sketch set size from the second parameters
    	    if(!( parameters[1] instanceof ConstantObjectInspector ) ) {
    	        throw new HiveException("Sketch Set size must be a constant");
    	    }
    	    ConstantObjectInspector sizeOI = (ConstantObjectInspector) parameters[1];
            this.sketchSetSize = ((IntWritable) sizeOI.getWritableConstantValue()).get();
    	  } else {
    	    sketchSetSize = DEFAULT_SKETCH_SET_SIZE;
          }
      } else { /// Mode m == Mode.PARTIAL2 || m == Mode.FINAL
    	   /// merge() gets called ... map is passed in ..
          if(sketchSetType == 1) {
              partialOI = (StandardListObjectInspector) parameters[0];
          } else {
              partialOI = (StandardListObjectInspector) parameters[0];
          }
        		 
      } 
      /// The intermediate result is a map of hashes and strings,
      /// The final result is an array of strings
      if( m == Mode.FINAL || m == Mode.COMPLETE) {
    	  /// for final result
         return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
      } else { /// m == Mode.PARTIAL1 || m == Mode.PARTIAL2
         if(sketchSetType == 1) {
             return ObjectInspectorFactory
                     .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableBinaryObjectInspector);
         } else {
             return ObjectInspectorFactory
                     .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableBinaryObjectInspector);
         }
      }
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      if(sketchSetType == 1) {
          CntAggregationBuffer ceb = new CntAggregationBuffer();
          reset(ceb);
          return ceb;
      } else {
        SketchSetBuffer buff= new SketchSetBuffer();
        buff.init(sketchSetSize);
        buff.setParamType(sketchSetType);
        return buff;
      }
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters)
        throws HiveException {

      if(parameters[0] == null)
          return;

      if(sketchSetType == 1) {
          CntAggregationBuffer ceb = (CntAggregationBuffer) agg;
          Object x = ObjectInspectorUtils.copyToStandardObject(parameters[0],
                  inputPrimitiveOI, ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
          Long value = (Long) x;
          ceb.hash.add(value);
      } else {
        Object strObj = parameters[0];
        String str = inputStrOI.getPrimitiveJavaObject( strObj);
        SketchSetBuffer myagg = (SketchSetBuffer) agg;
        myagg.addItem(str);
      }
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial)
        throws HiveException {
    	/// Partial is going to be a map of strings and hashes
        if (partial == null) {
            return;
        }

        if(sketchSetType == 1) {
            CntAggregationBuffer ceb = (CntAggregationBuffer) agg;
            TLongHashSet hh = null;
            try {
                List<BytesWritable> partialResult = (List<BytesWritable>) partialOI
                        .getList(partial);
                BytesWritable partialBytes = partialResult.get(0);
                ByteArrayInputStream bais = new ByteArrayInputStream(
                        partialBytes.getBytes());
                ObjectInputStream oi = new ObjectInputStream(bais);
                hh = (TLongHashSet) oi.readObject();
            } catch (Exception e) {
                throw new HiveException(e.getMessage());
            }
            mergeHashSets(hh, ceb);

        } else {
            SketchSetBuffer myagg = (SketchSetBuffer) agg;
            Map<Long,String> hh = null;
            try {
                List<BytesWritable> partialResult = (List<BytesWritable>) partialOI
                        .getList(partial);
                BytesWritable partialBytes = partialResult.get(0);
                ByteArrayInputStream bais = new ByteArrayInputStream(
                        partialBytes.getBytes());
                ObjectInputStream oi = new ObjectInputStream(bais);
                hh = (Map<Long,String>) oi.readObject();
            } catch(Exception e) {
                throw new HiveException(e.getMessage());
            }

            //// Place SKETCH_SIZE into the partial map ...
            if(myagg.getSize() == -1) {
                for( Map.Entry entry : hh.entrySet()) {
                    Long hash = (Long)entry.getKey(); //this.partialMapHashOI.get( entry.getKey());
                    String item = (String) entry.getValue(); //partialMapStrOI.getPrimitiveJavaObject( entry.getValue());
                    if(item.equals(SKETCH_SIZE_STR)) {
                        this.sketchSetSize = (int)hash.intValue();
                        myagg.init(sketchSetSize);
                        break;
                    }
                }
            }

            for( Map.Entry entry : hh.entrySet()) {
                Long hash = (Long)entry.getKey(); //this.partialMapHashOI.get( entry.getKey());
                String item = (String)entry.getValue(); //partialMapStrOI.getPrimitiveJavaObject( entry.getValue());
                if(!item.equals(SKETCH_SIZE_STR)) {
                    myagg.addHash(hash, item);
                }
            }
        }
    }

   private void mergeHashSets(TLongHashSet hh, CntAggregationBuffer ceb) {
       if (ceb.hash.size() == 0) {
           ceb.hash = hh;
           return;
       }
       if (ceb.hash.size() > hh.size()) {
           ceb.hash.addAll(hh);
       } else {
           hh.addAll(ceb.hash);
           ceb.hash = hh;
       }
   }



    @Override
    public void reset(AggregationBuffer buff) throws HiveException {
      if(sketchSetType == 1) {
        ((CntAggregationBuffer) buff).hash = new TLongHashSet();
      } else {
        SketchSetBuffer sketchBuff = (SketchSetBuffer) buff;
        sketchBuff.reset();
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      if(sketchSetType == 1) {
        CntAggregationBuffer ceb = (CntAggregationBuffer) agg;
        if (ceb.hash == null) {
          return null;
        }
        return new LongWritable(ceb.hash.size());
      } else {
        SketchSetBuffer myagg = (SketchSetBuffer) agg;
        long reach = myagg.getEstimatedReach();
        return new LongWritable(reach);
      }
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
        if(sketchSetType == 1) {
            CntAggregationBuffer ceb = (CntAggregationBuffer) agg;
            ByteArrayOutputStream b = new ByteArrayOutputStream();
            try {
                ObjectOutputStream o = new ObjectOutputStream(b);
                o.writeObject(ceb.hash);
            } catch (IOException e) {
                throw new HiveException(e.getMessage());
            }
            byte[] arr = b.toByteArray();
            List<BytesWritable> bl = new ArrayList<BytesWritable>();
            bl.add(new BytesWritable(arr));
            return bl;
        } else {
    	  SketchSetBuffer myagg = (SketchSetBuffer)agg;
          Map<Long,String> tempMap = myagg.getPartialMap();

          ByteArrayOutputStream b = new ByteArrayOutputStream();
          try {
            ObjectOutputStream o = new ObjectOutputStream(b);
            o.writeObject(tempMap);
          } catch (IOException e) {
            throw new HiveException(e.getMessage());
          }
          byte[] arr = b.toByteArray();
          List<BytesWritable> bl = new ArrayList<BytesWritable>();
          bl.add(new BytesWritable(arr));
          return bl;
        }
    }


    static class CntAggregationBuffer implements AggregationBuffer {
      TLongHashSet hash = new TLongHashSet();
    }
  }


}
