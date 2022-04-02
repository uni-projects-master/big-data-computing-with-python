from pyspark import SparkContext, SparkConf
import sys
import os
import random as rand

conf = SparkConf().setAppName('HomeWork1').setMaster("local[*]")
sc = SparkContext(conf=conf)

def format_partition(dataset, S, K=1):
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

def format_and_filter_dataset(dataset,K,S):
    filtered_dataset = dataset\
        .mapPartitions(lambda x: format_partition(x, S, K))\
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

def partial_count_2(index_product_pair):
    product_count2 = {}
    product_list = index_product_pair[1]
    for product in product_list:
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

def top(partition, H):
    top_H_element = set()
    list_of_element = []

    for x in partition:
        list_of_element.append(x)

    for i in range(min(len(list_of_element),H)):
        p = None
        n = -1
        for y in list_of_element:
            t_p = y[0]
            t_n = y[1]
            if n < t_n and ((t_p,t_n) not in top_H_element):
                p = t_p
                n = t_n
        top_H_element.add((p,n))
    return [x for x in top_H_element]

def topH(product_popularity, H, K=1):
    partitioned_top_H = product_popularity\
        .repartition(numPartitions=K)\
        .mapPartitions(lambda x : top(x,H))\
        .top(H, key= lambda x: x[1])
    return partitioned_top_H

def print_in_lex_order(product_popularity):
    return product_popularity.sortByKey()

def main():
    # CHECKING NUMBER OF CMD LINE PARAMETERS
    assert len(sys.argv) == 5, "Usage: python G042HW1.py <K> <H> <S> <file_name>"

    # SPARK SETUP
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

    #1) loading and partitioning the dataset
    rawData = sc.textFile(data_path, minPartitions=K).cache()
    rawData.repartition(numPartitions=K)
    print("number of transactions =", rawData.count())

    #2) filtering and converting the rawData to Product-Costumer pairs
    product_costumer = format_and_filter_dataset(rawData,K,S)
    print("number of product costumer =", product_costumer.count())

    #3) computing product popularity using mapByPartition
    product_popularity1 = popularity1(product_costumer,K)

    #4) computing product popularity using map and reduceByKey
    product_popularity2 = popularity2(product_costumer,K)

    #5) extrcting the top H most popular items
    if H > 0:
        topHValues = topH(product_popularity1,H,K)
        print("Top =", topHValues)

    #6) printing the product popularity dataset
    if H == 0:
        print("product popularity1 =", print_in_lex_order(product_popularity1).collect())
        print("product popularity2 =", print_in_lex_order(product_popularity2).collect())
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
