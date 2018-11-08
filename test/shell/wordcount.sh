
spark-submit --master yarn \
             --driver-memory 4G  \
             --executor-memory 2G \
             --total-executor-cores 14 \
            --deploy-mode cluster \
             /data/server/spark-2.2.0-bin-hadoop2.7/examples/src/main/python/wordcount.py \
             /spark/data/words.txt
             
