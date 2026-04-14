from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min

# Inicializar Spark
spark = SparkSession.builder \
    .appName("BatchProcessorDolar") \
    .getOrCreate()

# 1. Leer el archivo CSV
df = spark.read.csv("mcec-87by.csv.3", header=True, inferSchema=True)

# 2. Mostrar los primeros registros
print("Datos iniciales:")
df.show(5)

# 3. Limpieza de datos (eliminar nulos)
df = df.dropna()

# 4. Confirmar columnas
print("Columnas del dataset:", df.columns)

# 5. Análisis exploratorio
print("Estadísticas generales:")
df.describe().show()

# Promedio del valor
print("Promedio del valor del dólar:")
df.select(avg("valor")).show()

# Máximo y mínimo
print("Valor máximo del dólar:")
df.select(max("valor")).show()

print("Valor mínimo del dólar:")
df.select(min("valor")).show()

# Conteo de registros
print("Número total de registros:", df.count())

# 6. Visualizar resultados finales
print("Primeros 20 registros después de limpieza:")
df.show(20)

# Finalizar Spark
spark.stop()
