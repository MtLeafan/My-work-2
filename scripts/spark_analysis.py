from pyspark.sql import SparkSession

# Создаем SparkSession
spark = SparkSession.builder \
    .appName("DataAnalysis") \
    .getOrCreate()

# Загружаем данные из CSV-файла
df = spark.read.csv('data/employees.csv', header=True, inferSchema=True)

# Показываем данные
df.show()

# Фильтруем данные
it_employees = df.filter(df['department'] == 'IT')
it_employees.show()

# Группируем данные и считаем среднюю зарплату
avg_salary = df.groupBy('department').avg('salary')
avg_salary.show()

# Сохраняем результат в CSV
avg_salary.write.csv('data/avg_salary.csv')

# Останавливаем SparkSession
spark.stop()