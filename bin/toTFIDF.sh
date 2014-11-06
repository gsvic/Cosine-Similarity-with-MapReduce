mahout seqdirectory \
-i input2 \
-o input2-seq \
-c UTF-8 \
-xm sequential

mahout seq2sparse \
-i input2-seq \
-o input2-vectors \
-wt tfidf