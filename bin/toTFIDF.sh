mahout seqdirectory \
-i input \
-o input-seq \
-c UTF-8 \
-xm sequential

mahout seq2sparse \
-i input-seq \
-o input-vectors \
-wt tfidf
