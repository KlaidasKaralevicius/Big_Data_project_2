# dependencies
from pyspark.sql import SparkSession
import re

# init spark
spark = SparkSession.builder.appName('spark-lvl-1').getOrCreate()
spark

# read the file
rdd = spark.sparkContext.textFile('duom_full.txt')

# split data into blocks, spliting by block ending }}
blocks = rdd.flatMap(lambda line: line.split('}}')). \
                   filter(lambda block: block.strip() != '')

# transform blocks to start and end with single {}
def clean_blocks(block):
    block = block.strip()
    if block.startswith('{{'):
        block = block[1:]
    return block + '}'

formated_blocks = blocks.map(clean_blocks)

# transform rdd to dictionary
def to_dict(block):
    pairs = re.findall(r'\{([^}=]+)=([^}]*)\}', block)
    return {k.strip(): v.strip() for k, v in pairs}

dict_rdd = formated_blocks.map(to_dict)

# only leave important values in dictionary
filtered = dict_rdd.filter(lambda x: 'svoris' in x and 'svorio grupe' in x)

# map important values to (key, value) pairs
group_values = filtered.map(lambda x: (x['svorio grupe'], float(x['svoris'])))

# find min, max, sum and count of values in each partition
def single_stats(state, value):
    min_v, max_v, sum_v, count = state
    return min(min_v, value), max(max_v, value), sum_v + value, count + 1

# combine stats from different partitions
def combined_stats(state1, state2):
    return (
        min(state1[0], state2[0]),
        max(state1[1], state2[1]),
        state1[2] + state2[2],
        state1[3] + state2[3]
    )

# aggregate values by key
agg = group_values.aggregateByKey(
    (float('inf'), float('-inf'), 0.0, 0),
    single_stats,
    combined_stats
)

# apply values (min, max) and find average
result = agg.map(lambda x: (x[0], x[1][0], x[1][1], round(x[1][2] / x[1][3], 2))).collect()

# count all full blocks
total_blocks = dict_rdd.count()

# find blocks without keys
no_svoris = dict_rdd.filter(lambda x: 'svoris' not in x).count()
no_grupe = dict_rdd.filter(lambda x: 'svorio grupe' not in x).count()
no_both = dict_rdd.filter(lambda x: 'svoris' not in x and 'svorio grupe' not in x).count()

# print results
print('Stats')
for group, min, max, avg in result:
    print(f'{group}: min - {min}, max - {max}, avg - {avg}')

print(f'\nTotal entries: {total_blocks}')
print(f'No "svoris": {no_svoris}')
print(f'No "svorio grupe": {no_grupe}')
print(f'Missing both: {no_both}')

# stop spark
spark.stop()