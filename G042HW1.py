from pyspark import SparkContext, SparkConf
from pyspark.sql import DataFrameReader
import sys
import os
import random as rand

def format_and_filter_dataset(dataset, S, K=1):
    product_costumer = set()
    for string in dataset:
        fields = string.split(',')
        product = fields[1]
        count = int(fields[3])
        costumer = fields[6]
        country = fields[7]
        if (count > 0 and (country == S or S == "all")):
            product_costumer.add((product,costumer))
    return [((product,costumer),0) for (product,costumer) in product_costumer]

def remove_copies(pairs):
    return (pairs[0][0],pairs[0][1])

def filter(dataset,K,S):
    filtered_dataset = dataset\
        .mapPartitions(lambda x: format_and_filter_dataset(x, S, K))\
        .groupByKey()\
        .keys()
    return filtered_dataset

def partial_count(dataset):
    product_count = {}
    for (product, costumer) in dataset:
        if product not in product_count.keys():
            product_count[product] = 1
        else:
            product_count[product] += 1

    return [(product,product_count[product]) for product in product_count.keys()]

def full_count(pair):
    product = pair[0]
    count_list = pair[1]
    return (product, sum(count_list))

def popularity1(product_costumer, K=1):
    product_popularity1 = product_costumer\
        .repartition(numPartitions=K)\
        .mapPartitions(partial_count)\
        .groupByKey()\
        .mapValues(lambda partial_counts: sum(partial_counts))
    return product_popularity1

def random_partition(product_costumer_pair, K):
    return (rand.randint(0, K - 1), product_costumer_pair[0])

def partial_count_2(product_list):
    product_count2 = {}
    for (index,product) in product_list:
        if product not in product_count2.keys():
            product_count2[product] = 1
        else:
            product_count2[product] += 1

    return [(product, product_count2[product]) for product in product_count2.keys()]

def popularity2(product_costumer, K=1):
    product_popularity2 = product_costumer\
        .map(lambda x : random_partition(x,K))\
        .groupByKey()\
        .flatMap(partial_count_2)\
        .reduceByKey(lambda x, y: x + y)  # <-- REDUCE PHASE (R2)
    return product_popularity2

def main():
    # CHECKING NUMBER OF CMD LINE PARAMETERS
    assert len(sys.argv) == 5, "Usage: python G042HW1.py <K> <H> <S> <file_name>"

    # SPARK SETUP
    conf = SparkConf().setAppName('HomeWork1').setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # INPUT READING

    # 1. Read number of partitions
    K = sys.argv[1]
    assert K.isdigit(), "K must be an integer"
    K = int(K)

    # 2. Read H
    H = sys.argv[2]
    assert H.isdigit(), "H must be an integer"
    H = int(H)

    # 3. Read S
    S = sys.argv[3]

    # 4. Read input file and subdivide it into K random partitions
    data_path = sys.argv[4]
    assert os.path.isfile(data_path), "File or folder not found"
    #rawData = DataFrameReader.csv(DataFrameReader,data_path)

    rawData = sc.textFile(data_path, minPartitions=K).cache()
    rawData.repartition(numPartitions=K)
    # SETTING GLOBAL VARIABLES
    numdocs = rawData.getNumPartitions();
    print("Number of documents = ", numdocs)
    filteredRDD = filter(rawData,K,S)
    print("filtered stuff =", filteredRDD.collect())
    product_popularity1 = popularity1(filteredRDD)
    print("product popularity =", product_popularity1.collect())
    product_popularity2 = popularity2(filteredRDD)
    print("product popularity2 =", product_popularity2.collect())
'''
    # STANDARD WORD COUNT with reduceByKey
    print("Number of distinct words in the documents using reduceByKey =", word_count_1(docs).count())

    # IMPROVED WORD COUNT with groupByKey
    print("Number of distinct words in the documents using groupByKey =", word_count_2(docs, K).count())

    # IMPROVED WORD COUNT with groupBy
    print("Number of distinct words in the documents using groupBy =", word_count_3(docs, K).count())

    # WORD COUNT with mapPartitions
    wordcount = word_count_with_partition(docs)
    numwords = wordcount.count()
    print("Number of distinct words in the documents using mapPartitions =", numwords)

    # COMPUTE AVERAGE WORD LENGTH
    average_word_len = wordcount.keys().map(lambda x: len(x)).reduce(lambda x, y: x + y)
    print("Average word length = ", average_word_len / numwords)
'''

if __name__ == "__main__":
    main()
