/*
 * Copyright 2015 Giannakouris - Salalidis Victor
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

package com.gsvic.csmr.io;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.common.StringTuple;
import org.apache.mahout.math.VectorWritable;

public class InputData {
    
    /**
     * Reads a Hadoop Sequence File
     * @param conf
     * @param input
     * @return Returns the given sequence file in a HashMap
     * @throws IOException 
     */
    public static HashMap<Text,Text> readSequenceFile(Configuration conf, Path input) 
            throws IOException{
        
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader reader; 
        reader = new SequenceFile.Reader(fs,input,conf);
        
        HashMap<Text,Text> dcf = new HashMap<>();
        Text key = new Text();
        Text value = new Text();
        
        int count = 0;
        while (reader.next(key,value)){
            dcf.put(new Text(key), new Text(value));
            ++count;
        }
        return dcf;
    }
    
    /**
     * Reads a Vectorized Text File, tfidf vectors/tf vectors
     * @param conf
     * @param input
     * @return Returns the vectorized text file in a HashMap
     * @throws IOException 
     */
    public static HashMap<Text,VectorWritable> vectorizedTextReader(Configuration conf, Path input) 
            throws IOException{
        
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader reader; 
        reader = new SequenceFile.Reader(fs,input,conf);
        
        HashMap<Text,VectorWritable> dcf = new HashMap<>();
        Text key = new Text();
        VectorWritable value = new VectorWritable();
        
        while (reader.next(key,value)){
            dcf.put(new Text(key.toString()), new VectorWritable(value.get()));
        }
        
        return dcf;
    }
    
    /**
     * Reads the Document-Frequency file
     * @param conf
     * @param dfFile
     * @return Returns the Document-Frequency data in a HashMap
     * @throws IOException 
     */
    public static HashMap<IntWritable,LongWritable> readDf(Configuration conf, 
            Path dfFile) 
            throws IOException{
        
        FileSystem filesystem = FileSystem.get(conf);
        SequenceFile.Reader reader; 
        reader = new SequenceFile.Reader(filesystem,dfFile,conf);
        
        HashMap<IntWritable,LongWritable> dcf = new HashMap<>();
        IntWritable key = new IntWritable();
        LongWritable value = new LongWritable();
        
        while (reader.next(key,value)){
            dcf.put(new IntWritable(key.get()), new LongWritable(value.get()));
        }
        
        return dcf;
    }
   
    /**
     *  Reads the dictionary file
     * @param conf
     * @param dict
     * @return returns the dictionary in a HashMap
     * @throws IOException 
     */
    public static HashMap<Text,IntWritable> readDictionary( Configuration conf, 
            Path dict) 
            throws IOException{
        FileSystem filesystem = FileSystem.get(conf);        
        SequenceFile.Reader reader = new SequenceFile.Reader(filesystem,
                dict,conf);
        
        HashMap<Text,IntWritable> dictMap = new HashMap<>();
        Text key = new Text();
        IntWritable value = new IntWritable();
        
        while (reader.next(key,value)){
            dictMap.put(new Text(key), new IntWritable(value.get()));
        }
        
        return dictMap;
    }
    
    /**
     * Reads the tokenized document
     * @param conf
     * @param input
     * @return Returns the document tokens (StringTuples) in a HashMap
     * @throws IOException 
     */
    public  HashMap<Text, StringTuple> readTokenizedDocument(Configuration 
            conf, Path input) 
            throws IOException{
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader reader;
        reader = new SequenceFile.Reader(fs,input,conf);
        Text key = new Text();
        StringTuple value = new StringTuple();
        HashMap<Text, StringTuple> tokensMap = new HashMap<>();
        
        while (reader.next(key,value)){
            tokensMap.put(new Text(key), new StringTuple(value.getEntries()));
        }
        
        return tokensMap;
    }
    
}