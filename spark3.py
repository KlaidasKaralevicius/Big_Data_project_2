from pyspark.sql import SparkSession
import re
import time

spark = SparkSession.builder.appName('spark-lvl-3').getOrCreate()
spark

start = time.time()

rdd = spark.sparkContext.textFile('duom_full.txt')

blocks = rdd.flatMap(lambda line: line.split('}}')). \
                   filter(lambda block: block.strip() != '')

def clean_blocks(block):
    block = block.strip()
    if block.startswith('{{'):
        block = block[1:]
    return block + '}'

formated_blocks = blocks.map(clean_blocks)

def to_dict(block):
    pairs = re.findall(r'\{([^}=]+)=([^}]*)\}', block)
    return {k.strip(): v.strip() for k, v in pairs}

dict_rdd = formated_blocks.map(to_dict)

# extracting data
def extract_data(data):
    # get data from blocks (if missing make an empty string)
    zona = data.get('geografine zona', '').strip()
    diena = data.get('sustojimo savaites diena', '').strip()
    # set None if missing (becouse it numbers so can't make empty string, and some values have 0, so can't do that eihter)
    raw_siunta = data.get('siuntu skaicius')
    raw_klientas = data.get('Sustojimo klientu skaicius')

    # cound how many values are missing (if value is empty/None set 1-True, else 0-False)
    missing_zona = 1 if not zona else 0
    missing_day = 1 if not diena else 0
    missing_siunta = 1 if not raw_siunta else 0
    missing_klientas = 1 if not raw_klientas else 0

    # makes an integer (if there is value, make int, if not, set to 0)
    siunta = int(raw_siunta) if raw_siunta is not None else 0
    klientas = int(raw_klientas) if raw_klientas is not None else 0

    # make double key and return value tuple
    key = (zona, diena)
    return (key, (siunta, klientas, missing_zona, missing_day, missing_siunta, missing_klientas))

# map data
mapped_data = dict_rdd.map(extract_data)

# find and combine data (same action, differet parameters, first - same partition, second - different partition) 
def stats(a, b):
    return (
        a[0] + b[0], # siuntos
        a[1] + b[1], # klientai
        a[2] + b[2], # missing_zona
        a[3] + b[3], # missing_diena
        a[4] + b[4], # missing_siunta
        a[5] + b[5]  # missing_klientas
    )

agg = mapped_data.aggregateByKey(
    (0, 0, 0, 0, 0, 0), 
    stats, 
    stats)

# sorts values, so first have both keys, then one key, then neigher key 
sorted_rows = sorted(agg.collect(), key=lambda x: ( # collect combined data and convert to list
    # if both exist = 0, if one missing = 1, if both missing = 2
    (1 if not x[0][0] else 0) + (1 if not x[0][1] else 0), 
    # if multipple rows missing key, sort by zona then diena
    x[0][0] or '',
    x[0][1] or ''
    )
)

# prep variables for table output (and missing value count)
output = []
nr = 1
total_missing_zona = 0
total_missing_diena = 0 
total_missing_siunta = 0
total_missing_klientas = 0

# set data for
for (zona, diena), (siunta, klientas, missing_zona, missing_diena, missing_siunta, missing_klientas) in sorted_rows:
    # if zona or diena missing set to '-'
    zona_table = zona if zona else '-'
    diena_table = diena if diena else '-'
    # save data to row -> nr zona diena siunta klientas
    output.append([nr, zona_table, diena_table, siunta, klientas])
    # increase row number
    nr += 1

    # count missing values
    total_missing_zona += missing_zona
    total_missing_diena += missing_diena
    total_missing_siunta += missing_siunta
    total_missing_klientas += missing_klientas

# save last row info
output.append(['missing values', total_missing_zona, total_missing_diena, total_missing_siunta, total_missing_klientas])

end = time.time()

# print data
for row in output:
    # output all rows
    print('\t'.join(str(x) for x in row))

# output time for the code
print(f'\tcompleted in {round(end - start, 2)} seconds')