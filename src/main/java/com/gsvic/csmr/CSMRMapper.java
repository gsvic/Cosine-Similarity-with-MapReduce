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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VectorWritable;


public class CSMRMapper extends Mapper<Text,VectorWritable,IntWritable,
        DocumentWritable >{
    
    @Override
    public void map(Text key, VectorWritable value, Context context) 
            throws IOException, InterruptedException{   
            DocumentWritable p = new DocumentWritable(new Text(key.toString())
                    ,new VectorWritable(value.get()));
            context.write(new IntWritable(1),p);

    }
}
