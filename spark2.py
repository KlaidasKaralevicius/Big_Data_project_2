from pyspark.sql import SparkSession
import re

spark = SparkSession.builder.appName('spark-lvl-2').getOrCreate()
spark

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

# find blocks with missing key or values
total_blocks = dict_rdd.count()
no_marsrutas = dict_rdd.filter(lambda x: 'marsrutas' not in x).count()
no_geozona = dict_rdd.filter(lambda x: 'geografine zona' not in x).count()
no_date = dict_rdd.filter(lambda x: 'sustojimo data' not in x).count()
no_all = dict_rdd.filter(lambda x: 'marsrutas' not in x and 'geografine zona' not in x and 'sustojimo data' not in x).count()

# collect only important key and values + check for all keys to have values
filtered = dict_rdd.filter(lambda x: all(y in x and x[y].strip() != '' for y in ['marsrutas', 'geografine zona', 'sustojimo data']))
# map information to (key, (zone, date))
route_info = filtered.map(lambda x: (x['marsrutas'], (x['geografine zona'], x['sustojimo data'])))
# group by key
group_values = route_info.groupByKey().mapValues(list)

# analyze route information to find multiple zones and connection between days
def analyze_route(pairs):
    zones = set()
    date_zone_map = {}
    for zone, date in pairs:
        # if zone is not seen before, add it to list
        zones.add(zone)
        if date not in date_zone_map:
            date_zone_map[date] = set()
        date_zone_map[date].add(zone)
    # checks if route has multiple zones (more than 1)
    has_multiple_zones = len(zones) > 1
    # filters date_zone_map to only keep dates where more than 1 zone was visited
    multi_day_zone = {date: list(x) for date, x in date_zone_map.items() if len(x) > 1}
    return {
        'zones': list(zones),
        'multi_zone': has_multiple_zones,
        'same_day_multi_zone': multi_day_zone
    }

# get data from route information
result = group_values.mapValues(analyze_route).collectAsMap()

# go through all routes and only keep which have multi_zone=True
multi_zone_routes = {x: y['zones'] for x, y in result.items() if y['multi_zone']}
# find routes that have multiple zones on same day (output day and what rotes were visited on that day)
multi_zone_same_day = {x: y['same_day_multi_zone'] for x, y in result.items() if y['same_day_multi_zone']}

# save output to file
with open('output.txt', 'w', encoding='utf-8') as f:
    f.write(f'Total blocks: {total_blocks}\n')
    f.write(f'Missing marsrutas: {no_marsrutas}\n')
    f.write(f'missing geozona: {no_geozona}\n')
    f.write(f'missing date: {no_date}\n')
    f.write(f'missing all: {no_all}\n\n')

    f.write('Routes with mutiple zones:\n')
    for route, zones in multi_zone_routes.items():
        f.write(f'Route: {route}, zones: {zones}\n')

    f.write('\nRoutes with mutiple zones on the same day:\n')
    for route, day_info in multi_zone_same_day.items():
        for date, zones in day_info.items():
            f.write(f'route {route} on {date}: zones - {zones}\n')

spark.stop()