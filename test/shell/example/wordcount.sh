
spark-submit --master yarn \
             --driver-memory 4G  \
             --executor-memory 2G \
             --total-executor-cores 14 \
            --deploy-mode cluster \
            /data/code/zhanqi_recommend_system/test/model/example/wordcount.py \
             /spark/data/words.txt
             
