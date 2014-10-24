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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import com.gsvic.csmr.*;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.mahout.math.VectorWritable;

public class main {
    public static void main(String[] args) 
            throws IOException, InterruptedException, ClassNotFoundException{
        Configuration conf = new Configuration();
        Job job;
        Path input, output;
        input = new Path(args[0]);
        output = new Path(args[1]);
        
        job = new Job();
        job.setJobName("CSMR Job");
        job.setJarByClass(main.class);
        
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);        
        
        job.setMapperClass(CSMRMapper.class);
        job.setReducerClass(CSMRReducer.class); 

        
        job.setInputFormatClass(SequenceFileInputFormat.class);
        
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DocumentWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VectorWritable.class);


        
        
        
        System.exit( job.waitForCompletion(true) ? 1 : 0 );
    }
    
}
