package brickhouse.udf.sketch;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import gnu.trove.set.hash.THashSet;
import gnu.trove.set.hash.TLongHashSet;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;

import brickhouse.analytics.uniques.SketchSet;

class SketchSetBuffer implements AggregationBuffer {
	private SketchSet sketchSet = null;
//  public THashSet hash = null;
  public MergedHashes merged = null;
  private int paramType = 0;
  public int threshold = 5000;

  public void init(int size) {
    if(merged == null)
      merged = new MergedHashes();

    if( sketchSet == null ||
        ((sketchSet.getMaxItems() != size) && (size != -1))) {
      sketchSet = new SketchSet( size);
    } else {
      sketchSet.clear();
    }
  }

	public void init(int size, int threshold) {
    if(merged == null)
      merged = new MergedHashes();

		if( sketchSet == null ||
		    ((sketchSet.getMaxItems() != size) && (size != -1))) {
			sketchSet = new SketchSet( size);
		} else {
			sketchSet.clear();
		}
    this.threshold = threshold;
    if(threshold > 0)
      merged.hash = new THashSet();
	}

	public void reset() {
	  sketchSet.clear();
    merged = new MergedHashes();
    merged.hash = new THashSet();
	}
	
	public int getSize() {
	  if ( sketchSet != null) {
	      return sketchSet.getMaxItems();
	  } else {
	    return -1;
	  }
	}

  public void setParamType(int type) {
        paramType = type;
    }

  public long getEstimatedReach()
  {
    if(sketchSet != null) {
        try {
            if(paramType == 0) {
                return (long)SketchSet.EstimatedReach(sketchSet.lastItem(), sketchSet.getMaxItems());
            } else {
                return (long)SketchSet.EstimatedReach(sketchSet.lastHash(), sketchSet.getMaxItems());
            }
        } catch(Exception e) {
            return -1;
        }
    } else {
      return -1;
    }
  }
	
	public List<String> getSketchItems() {
    return sketchSet.getMinHashItems();
	}
	
  public MergedHashes getPartialMap() {
	  merged.sketch =  (TreeMap<Long,String>)sketchSet.getTreeItemMap();
	  merged.sketch.put( (long)sketchSet.getMaxItems(), SketchSetUDAF.SKETCH_SIZE_STR);
	  return merged;
  }
    
  public void addItem( String str) {
    sketchSet.addItem( str) ;
    if(merged.hash != null && merged.hash.size() < threshold)
      merged.hash.add(str);
  }

  public void addHash( Long hash, String str) {
  	sketchSet.addHashItem( hash, str );
  }

}
