from pyspark import SparkContext, SparkConf
import random as rand
import psutil
import os
import sys
from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


# Part 2
# Format and filtering process of the dataset from RDD of string to RDD of unique product_costumer

def format_partition(dataset, S):
    # by using a set we do already a part or the filtering
    product_costumer = set()

    for string in dataset:

        # we extract the product id, the quantity, the costumer id and country of the transaction
        fields = string.split(',')
        product = fields[1]
        quantity = int(fields[3])
        costumer = fields[6]
        country = fields[7]

        # we filter based product and id based on the following proposition
        if quantity > 0 and (country == S or S == "all"):
            # by using the set add function we only get distinct value from this partition
            product_costumer.add((product, costumer))

    return [((product, costumer), 0) for (product, costumer) in product_costumer]


def format_and_filter_dataset(dataset, S):
    filtered_dataset = (dataset
                        .mapPartitions(lambda x: format_partition(x, S))  # <-- MAP PHASE (R1)
                        .groupByKey()  # <-- SHUFFLE + GROUPING
                        .keys())  # <-- REDUCE PHASE (R1)
    return filtered_dataset


# Part 3
# partial sum computes the partial count of product_costumer with same product in a partition
def partial_count(dataset):
    product_count = {}
    for (product, costumer) in dataset:
        if product not in product_count.keys():
            product_count[product] = 1
        else:
            product_count[product] += 1
    return [(product, product_count[product]) for product in product_count.keys()]


# Implementation of product popularity using MapPartition and MapValues
def compute_popularity_1(product_costumer, K=1):
    product_popularity1 = (product_costumer
                           .repartition(numPartitions=K)  # <-- MAP PHASE (R1)
                           .mapPartitions(partial_count)  # <-- GROUPING + REDUCE PHASE (R1)
                           .groupByKey()  # <-- SHUFFLE + GROUPING
                           .mapValues(lambda partial_counts: sum(partial_counts)))  # <-- REDUCE PHASE (R2)
    return product_popularity1


# Part 4
# Implementation of product popularity using Map and ReduceByKey
def compute_popularity_2(product_costumer, K=1):
    product_popularity2 = (product_costumer
                           .map(lambda x: (x[0], 1))  # <-- MAP PHASE (R1)
                           .reduceByKey(lambda x, y: x + y, numPartitions=K))  # <-- REDUCE PHASE (R1) + (R2),
    # R1 works on partition and compute partial sums, R2 works on results of partition and compute full sums
    return product_popularity2


# Part 5
# top_H_reduce takes a partition of product popularity and a value H and return the H key value pair in
# the partition with the highest popularity
def top_H_reduce(partition, H):
    list_of_element = []

    for x in partition:
        list_of_element.append(x)

    if len(list_of_element) <= H:
        return [x for x in list_of_element]
    else:
        top_H_element = set()
        for i in range(H):
            p = None
            n = -1
            for y in list_of_element:
                t_p = y[0]
                t_n = y[1]
                if n < t_n and ((t_p, t_n) not in top_H_element):
                    p = t_p
                    n = t_n
            list_of_element.remove((p, n))
            top_H_element.add((p, n))
        return [x for x in top_H_element]


# We extract the top value in two round
# in the first round we partition the data, and we extract top H value from each partition
# if H is bigger than the size of the partition we return all values.
# We then have the RDD with all the TOP H values of each partition, and we get
# the top H value out of this
def topH(product_popularity, H, K=1):
    partitioned_top_H = (product_popularity
                         .repartition(numPartitions=K)
                         .mapPartitions(lambda x: top_H_reduce(x, H))  # <-- MAP PHASE
                         .top(H, key=lambda x: x[1]))  # <-- REDUCE PHASE
    return partitioned_top_H


# Part 6 WE CAN DELETE THIS IF WE USE "sorted()"
def print_in_lex_order(product_popularity):
    return product_popularity.sortByKey()


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
    # rawData = DataFrameReader.csv(DataFrameReader,data_path)

    # 1) loading and partitioning the dataset
    rawData = sc.textFile(data_path, minPartitions=K).cache()
    rawData.repartition(numPartitions=K)
    print("Number of rows = ", rawData.count())

    # 2) filtering and converting the rawData to Product-Costumer pairs
    product_costumer = format_and_filter_dataset(rawData, S)
    print("Product-Customer Pairs = ", product_costumer.count())

    # 3) computing product popularity using mapByPartition
    product_popularity_1 = compute_popularity_1(product_costumer, K)

    # 4) computing product popularity using map and reduceByKey
    product_popularity_2 = compute_popularity_2(product_costumer, K)

    # 5) extracting the top H most popular items
    if H > 0:
        topHValues = topH(product_popularity_1, H, K)
        print("Top ", H, " Products and their Popularities:")
        for i in topHValues:
            print("Product: ", i[0], " Popularity: ", i[1], end="; ")

    # 6) printing the sorted pairs of product-popularity
    if H == 0:
        productPopularity1 = sorted(product_popularity_1.collect())
        productPopularity2 = sorted(product_popularity_2.collect())

        print("productPopularity1:")
        for i in productPopularity1:
            print("Product: ", i[0], " Popularity: ", i[1], end="; ")

        print("productPopularity2:")
        for j in productPopularity2:
            print("Product: ", j[0], " Popularity: ", j[1], end="; ")


if __name__ == "__main__":
    main()
