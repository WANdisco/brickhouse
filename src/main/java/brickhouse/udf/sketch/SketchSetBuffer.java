package brickhouse.udf.sketch;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;

import brickhouse.analytics.uniques.SketchSet;

class SketchSetBuffer implements AggregationBuffer {
	private SketchSet sketchSet = null;
    private int paramType = 0;
	

	public void init(int size) {
		if( sketchSet == null || 
		    ((sketchSet.getMaxItems() != size) && (size != -1))) {
			sketchSet = new SketchSet( size);
		} else {
			sketchSet.clear();
		}
	}
	public void reset() {
	  sketchSet.clear();
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
	
    public Map<Long,String> getPartialMap() {
	    Map<Long,String> partial =  sketchSet.getHashItemMap();
	    partial.put( (long)sketchSet.getMaxItems(), SketchSetUDAF.SKETCH_SIZE_STR);
	    return partial;
    }
    
    public void addItem( String str) {
       sketchSet.addItem( str) ;
    }
    public void addHash( Long hash, String str) {
    	sketchSet.addHashItem( hash, str );
    }
}
