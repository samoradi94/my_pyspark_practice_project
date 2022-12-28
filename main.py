import pyspark


sc = pyspark.SparkContext('local[*]')


# textFile read an entire CSV record as a String
txt = sc.textFile('sample_data.csv')
print(txt.count())
print(type(txt))

python_lines = txt.filter(lambda line: '50000' in line.lower())
print(python_lines.count())