/*
 * Copyright 2014 Giannakouris - Salalidis Victor
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gsvic.csmr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.VectorWritable;

public final class DocumentWritable implements Writable {
    private Text key;
    private VectorWritable value;
    
    public DocumentWritable(){
        key = new Text();
        value = new VectorWritable();
    }
    public DocumentWritable(Text key, VectorWritable value){
        this.key = new Text(key.toString());
        this.value = new VectorWritable(value.get());
    }
    
    @Override
    public void readFields(DataInput in) throws IOException{
        key.readFields(in);
        value.readFields(in);
    }
    @Override
    public void write(DataOutput out)throws IOException{
        key.write(out);
        value.write(out);
    }
    
    public Text getKey(){
        return this.key;
    }
    public VectorWritable getValue(){
        return this.value;
    }
    
 
    @Override
    public String toString(){
        return this.key.toString()+"\n"+this.value.toString();
    }
    

}
