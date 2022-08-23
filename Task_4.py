import findspark
findspark.init()
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Window

spark = SparkSession.builder.config('spark.driver.extraClassPath', '/home/ivannikalayeu/Downloads/postgresql-42.4.2.jar')\
.config("spark.executor.instances", "2")\
.config("spark.executor.memory", "1g")\
.config('spark.executor.cores','1')\
.config("spark.driver.memory", "8g").getOrCreate()

url = 'jdbc:postgresql://localhost:5432/don'
properties = {'user': 'postgres', 'password': 'qwerty'}

# Вывести количество фильмов в каждой категории, отсортировать по убыванию.

df_category = spark.read.jdbc(url=url, table = 'category', properties=properties)
df_film_category = spark.read.jdbc(url=url, table='film_category', properties=properties)

df_rez_1 = (
    df_film_category
    .join(df_category, df_film_category.category_id == df_category.category_id, how='inner')
    .select(F.col('name'))
    .groupby(F.col('name'))
    .count()
    .orderBy(F.col('count').desc())
    .show()
)

# Вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.
df_actor = (
    spark.read.jdbc(url=url, table = 'actor', properties=properties)
    .select(F.col('actor_id'), F.col('first_name'), F.col('last_name'))
)


df_film_actor = (
    spark.read.jdbc(url=url, table = 'film_actor', properties=properties)
    .select(F.col('actor_id'), F.col('film_id'))
)

df_inventory = (
    spark.read.jdbc(url=url, table = 'inventory', properties=properties)
    .select(F.col('inventory_id'), F.col('film_id'))
)

df_rental = (
    spark.read.jdbc(url=url, table='rental', properties=properties)
    .select(F.col('rental_id'), F.col('inventory_id'))

)

df_rez_2 = (
    df_actor.alias('A')
        .join(df_film_actor.alias('B'), df_actor.actor_id == df_film_actor.actor_id, how='inner')
        .join(df_inventory, df_film_actor.film_id == df_inventory.film_id, how='inner')
        .join(df_rental, df_inventory.inventory_id == df_rental.inventory_id, how='inner')
        .groupby(F.col('A.actor_id'), F.col('A.first_name'), F.col('last_name'))
        .count()
        .orderBy(F.col('count').desc())
        .show(10)
)

# Вывести категорию фильмов, на которую потратили больше всего денег.

df_category = (
    spark.read.jdbc(url=url, table='category', properties=properties)
    .select(F.col('category_id'), F.col('name'))
)

df_film_category = (
    spark.read.jdbc(url=url, table='film_category', properties=properties)
    .select(F.col('film_id'), F.col('category_id'))
)

df_inventory = (
    spark.read.jdbc(url=url, table='inventory', properties=properties)
    .select(F.col('film_id'), F.col('inventory_id'))
)

df_rental = (
    spark.read.jdbc(url=url, table='rental', properties=properties)
    .select(F.col('rental_id'), F.col('inventory_id'))
)

df_payment = (
    spark.read.jdbc(url=url, table='payment', properties=properties)
    .select(F.col('rental_id'), F.col('amount'))
)

df_rez_3 = (
    df_category.alias('A')
    .join(df_film_category.alias('B'), df_category.category_id == df_film_category.category_id, how='inner')
    .join(df_inventory, df_film_category.film_id == df_inventory.film_id, how='inner')
    .join(df_rental, df_inventory.inventory_id == df_rental.inventory_id, how='inner')
    .join(df_payment, df_rental.rental_id == df_payment.rental_id, how='inner')
    .groupby(F.col('A.name'))
    .sum('amount')
#     .withcolumnrenamed('sum('amount')' ')
#     .orderby(F.col('sum').desc())
    #.agg(F.sum('amount').desc())
    .show(1)
)


# Вывести названия фильмов, которых нет в inventory.

df_film = (
    spark.read.jdbc(url=url, table='film', properties=properties)
    .select(F.col('film_id'), F.col('title'))
)
df_inventory = (
    spark.read.jdbc(url=url, table='inventory', properties=properties)
    .select(F.col('film_id'), F.col('store_id'))
)

df_rez_4 = (
    df_film.alias('A')
    .join(df_inventory.alias('B'), df_film.film_id == df_inventory.film_id, how='leftanti')
   .show()
)

# Вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. Если у нескольких актеров одинаковое кол-во фильмов, вывести всех..

df_query = (
    df_actor.alias('A')
    .join(df_film_actor.alias('B'), df_actor.actor_id == df_film_actor.actor_id, how='inner')
    .join(df_film, df_film_actor.film_id == df_film.film_id, how='inner')
    .join(df_film_category, df_film.film_id == df_film_category.film_id, how='inner')
    .join(df_category, df_film_category.category_id == df_category.category_id, how='inner')
    .filter(F.col('name') == 'Children')

)

df_condition = (
    df_query
    .groupby(F.col('A.actor_id'))
    .count()
    .orderBy(F.col('count').desc())
    .limit(3)
    .orderBy(F.col('count'))
    .limit(1)
    .select(F.col('count'))
    .rdd.flatMap(lambda x: x).collect()
)

rate = int(df_condition[0])

df_rez_5 = (
    df_query
    .groupby(F.col('A.actor_id'), F.col('first_name'), F.col('Last_name'))
    .count()
    .orderBy(F.col('count').desc())
    .filter(F.col('count') >= rate)
    .show()
)

# Вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1).
# Отсортировать по количеству неактивных клиентов по убыванию.

df_city = (
    spark.read.jdbc(url=url, table='city', properties=properties)
    .select(F.col('city_id'), F.col('city'))
)

df_address = (
    spark.read.jdbc(url=url, table='address', properties=properties)
    .select(F.col('city_id'), F.col('address_id'))
)

df_customer = (
    spark.read.jdbc(url=url, table='customer', properties=properties)
    .select(F.col('address_id'), F.col('active'))
)

df_rez_6 = (
    df_city
    .join(df_address, df_city.city_id == df_address.city_id, how='inner')
    .join(df_customer, df_address.address_id == df_customer.address_id, how='inner')
    .groupby(F.col('city'))
    .agg(
        F.sum(F.col('active')).alias('num_active_cust'),
        (F.count(F.col('active'))-F.sum(F.col('active'))).alias('num_not_active')
    )
    .orderBy(F.col('num_not_active').desc())
    .show()
)

# Вывести категорию фильмов,
# у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city),
# и которые начинаются на букву “a”. Тоже самое сделать для городов в которых есть символ “-”.


df_rental = (
    spark.read.jdbc(url=url, table='rental', properties=properties)
    .select(F.col('rental_date'), F.col('inventory_id'), F.col('return_date'), F.col('customer_id'))
)

df_inventory = (
    spark.read.jdbc(url=url, table='inventory', properties=properties)
    .select(F.col('inventory_id'), F.col('film_id'))
)

df_customer = (
    spark.read.jdbc(url=url, table='customer', properties=properties)
    .select(F.col('customer_id'), F.col('address_id'))
)

df_address = (
    spark.read.jdbc(url=url, table='address', properties=properties)
    .select(F.col('address_id'), F.col('city_id'))
)

df_city = (
    spark.read.jdbc(url=url, table='city', properties=properties)
    .select(F.col('city_id'), F.col('city'))
)

w_1 = Window().partitionBy('city')

df_rez_7 = (
    df_rental
    .join(df_inventory, df_inventory.inventory_id == df_rental.inventory_id, how='inner')
    .join(df_film_category, df_film_category.film_id == df_inventory.film_id, how='inner')
    .join(df_category, df_category.category_id == df_film_category.category_id, how='inner')
    .join(df_customer, df_customer.customer_id == df_rental.customer_id, how='inner')
    .join(df_address, df_address.address_id == df_customer.address_id, how='inner')
    .join(df_city, df_city.city_id == df_address.city_id, how='inner')
    .filter(F.col('city').rlike('^[A]'))
    .filter(F.col('city').rlike('-'))
    .groupby(F.col('name'), F.col('city'))

    .agg(
        F.sum(
            F.datediff(F.col('return_date'), F.col('rental_date'))
        ).alias('rental')
    )
    .withColumn('Max_rental', F.max(F.col('rental')).over(w_1))
    .filter(F.col('rental') == F.col('Max_rental'))
    .orderBy(F.col('Max_rental').desc())
    .show()
)



