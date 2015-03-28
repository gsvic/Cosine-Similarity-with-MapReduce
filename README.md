CSMR
====

Cosine Similarity with MapReduce

Description
====
This repository illustrates the implementation of CSMR algorithm. The paper illustrating CSMR algorithm has been published  with title "CSMR: A Scalable Algorithm for Text Clustering with Cosine Similarity and MapReduce" in the Artificial Intelligence Applications and Innovations 2014 (AIAI 2014) conference.

Paper
====
Link: http://link.springer.com/chapter/10.1007%2F978-3-662-44722-2_23

Instructions
====
* Go to the CSMR directory: `cd Cosine-Similarity-with-MapReduce`
* Build CSMR: `mvn install`
* Add your input folder (name it 'input') with the documents in raw format in Cosine-Similarity-with-MapReduce/bin
* Run CSMR: `./run-csmr.sh`
* See the results: `cat OUTPUT_FOLDER/Results/part-r-00000`

Related Links
====
* http://hadoop.apache.org/
* https://mahout.apache.org/
* http://en.wikipedia.org/wiki/Tf%E2%80%93idf
* http://en.wikipedia.org/wiki/Cosine_similarity
