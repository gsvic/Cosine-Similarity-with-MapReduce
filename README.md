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
* Convert your input documents folder into hadoop sequence files.
* Clone the repository in a local folder: `git clone git@github.com:gsvic/CSMR.git`
* Go to the directory CSMR: `cd CSMR`
* Build CSMR: `mvn install`
* Run CSMR: `hadoop -jarCSMR-0.1-jar-with-dependencies.jar main TFIDF_VECTORS_FOLDER/part-m-00000 OUTPUT_FOLDER`
* See the results: `cat OUTPUT_FOLDER/part-r-00000`

Related Links
====
* http://hadoop.apache.org/
* https://mahout.apache.org/
* http://en.wikipedia.org/wiki/Tf%E2%80%93idf
* http://en.wikipedia.org/wiki/Cosine_similarity
