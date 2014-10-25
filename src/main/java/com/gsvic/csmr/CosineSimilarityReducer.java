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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.math.VectorWritable;

public class CosineSimilarityReducer 
    extends Reducer<Text, VectorArrayWritable, Text, DoubleWritable>{
    
    @Override
    public void reduce(Text key, Iterable<VectorArrayWritable> value, Context context)
    throws IOException, InterruptedException{
        
        CosineDistanceMeasure cdm = new CosineDistanceMeasure();
        VectorWritable docX,docY;
        double cosine;
        for (VectorArrayWritable v : value){
            docX = (VectorWritable)v.get()[0];
            docY = (VectorWritable)v.get()[1];
            cosine = cdm.distance(docX.get(), docY.get());
            context.write(key, new DoubleWritable(cosine));
        }
    
    }
    
}
