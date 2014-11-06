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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class CSMRBase {
    private static String path;
    
    public static void generatePairs(String in, String out) 
            throws IOException,InterruptedException,ClassNotFoundException{
        
        Configuration conf = new Configuration();
        path = out;
        Job job;
        Path input, output;
        input = new Path(in);
        output = new Path(path+"/CSMRPairs");
        
        job = new Job(conf);
        job.setJobName("CSMR Pairs Job");
        job.setJarByClass(CSMRBase.class);
        
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);        
        
        job.setMapperClass(CSMRMapper.class);
        job.setReducerClass(CSMRReducer.class); 

        
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DocumentWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VectorArrayWritable.class);

        job.waitForCompletion(true);
    }
    
    public static void StartCSMR() 
            throws IOException,InterruptedException, ClassNotFoundException{
        
        Configuration conf = new Configuration();
        Job job;
        job = new Job(conf);
        job.setJobName("CSMR Cosine Similarity Job");
        job.setJarByClass(CSMRBase.class);
        
        FileInputFormat.addInputPath(job, 
                new Path(path+"/CSMRPairs/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path(path+"/Results"));        
        job.setMapperClass(Mapper.class);
        job.setReducerClass(CosineSimilarityReducer.class); 

        
        job.setInputFormatClass(SequenceFileInputFormat.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(VectorArrayWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        System.exit( job.waitForCompletion(true) ? 1 : 0 );
        
    }
}
