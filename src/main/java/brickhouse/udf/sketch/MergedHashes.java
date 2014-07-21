package brickhouse.udf.sketch;

import gnu.trove.set.hash.THashSet;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.TreeMap;

/**
 * Created by jjb on 7/21/14.
 */
public class MergedHashes implements Externalizable {
  public TreeMap<Long,String> sketch;
  public THashSet hash;

  public MergedHashes() {

  }

  public void writeExternal(ObjectOutput out) throws IOException {
    int cnt = 0;

    if(sketch != null && sketch.size() > 0)
      cnt |= 2;

    if(hash != null && hash.size() > 0)
      cnt |= 4;

    out.writeInt(cnt);

      if((cnt & 2) != 0)
        out.writeObject(sketch);
      if((cnt & 4) != 0)
        out.writeObject(hash);
  }

  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    int cnt = in.readInt();

      if((cnt & 2) != 0)
        sketch = (TreeMap<Long,String>)in.readObject();
      if((cnt & 4) != 0)
        hash = (THashSet)in.readObject();
  }
}