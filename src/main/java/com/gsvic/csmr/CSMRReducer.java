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

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VectorWritable;

public class CSMRReducer extends Reducer<IntWritable, DocumentWritable ,Text, 
        VectorArrayWritable> {
    
    private ArrayList<DocumentWritable> al;
    private VectorWritable[] val;
    private VectorArrayWritable vaw; 
    
    @Override
    public void reduce(IntWritable key, Iterable<DocumentWritable> values, 
            Context context) throws IOException, InterruptedException{
        al = new ArrayList();
        vaw = new VectorArrayWritable();
        
        /* Storing each key-value pair (document) in a java.util.ArrayList */
        for (DocumentWritable v : values){
            al.add(new DocumentWritable(v.getKey(),v.getValue()));
        }
        
        /* Generating all the possible combinations of documents */
        if (al.size()>0){
            for (int i=0;i<al.size();++i){
                for (int j=i+1;j<al.size();++j){
                    val = new VectorWritable[2];
                    
                    /* Generating the key for the current document pair with
                        the format "doci_name@docj_name" */
                    String k = al.get(i).getKey().toString()+
                            "@"+al.get(j).getKey().toString();
                    
                    //First Document (doci)
                    val[0] = new VectorWritable(al.get(i).getValue().get());
                    //Second Document (docj)
                    val[1] = new VectorWritable(al.get(j).getValue().get());
                    vaw.set(val);

                    context.write(new Text(k), vaw);
                }
            }  
        }
    }
  
}

