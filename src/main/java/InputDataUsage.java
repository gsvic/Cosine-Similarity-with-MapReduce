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

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.VectorWritable;
import com.gsvic.csmr.io.InputData;


public class InputDataUsage {
	public static void main(String[] argv) throws IOException{
		Configuration conf = new Configuration();
		Path in = new Path("tfidf-vectors/part-r-00000");
		HashMap<Text, VectorWritable> doc = InputData.vectorizedTextReader(conf, in);

		for (java.util.Map.Entry<Text, VectorWritable> entry:doc.entrySet()){
			System.out.println("Document ID: " + entry.getKey());
			System.out.println("Vector: "+entry.getValue());
			System.out.println("Dimensions: "+entry.getValue().get().size());
			break;
		}
	}
}
