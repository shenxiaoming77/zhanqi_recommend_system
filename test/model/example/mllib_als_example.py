#coding=utf-8

from  pyspark import  SparkContext
import  sys
from  pyspark.mllib.recommendation import Rating
from  pyspark.mllib.recommendation import ALS
from  pyspark.mllib.recommendation import MatrixFactorizationModel

if __name__ == '__main__':

    if(len(sys.argv) < 2):
        print("Usage: als recommend example <file>", file=sys.stderr)
        exit(-1)

    sparkContext = SparkContext(appName = 'mllib_als_example_python')

    lines = sparkContext.textFile(sys.argv[1])
    rates = lines.map(lambda x : x.split('\t')[0:3])
    rates_rdd = rates.map(lambda x : Rating(int(x[0]),int(x[1]), int(x[2])))
    print('rates rdd data: first')
    print(rates_rdd.first())

    #model train
    model = ALS.train(ratings=rates_rdd,
                      rank=20,
                      iterations=10,
                      lambda_=0.02)

    #model predict
    print('predict the score of product 20 from user 38:')
    print (model.predict(38, 20))

    #recommend product
    print('recommend top 10 products for user 38:')
    print(model.recommendProducts(38, 10))

    #recommend user
    print('recommend top 10 users for user 20:')
    print(model.recommendUsers(20, 10))

    print('recommend top 3 products for every user.......')
    print (model.recommendProductsForUsers(3).collect())

    print('recommend top 3 users for every product.......')
    print (model.recommendUsersForProducts(3).collect())

    sparkContext.stop()