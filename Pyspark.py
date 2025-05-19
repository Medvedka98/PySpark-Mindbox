from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

def get_product_category_pairs_and_unmatched_products(products_df, categories_df, product_categories_df):
    '''
    products_df: Информация о продуктах.
    categories_df: Информация о категориях.
    product_categories_df: Связывает продукты и категории.
    result_df: Результирующая таблица. Столбец "unmatched_product" выводит True если продукт не имеет категорий, иначе False.
    '''
    # 1. Соединяем продукты и категории через таблицу связей
    product_category_joined_df = product_categories_df.join(
        products_df, product_categories_df.product_id == products_df.product_id, "left"
    ).drop(products_df.product_id)  # чтобы не было дублирования столбца product_id

    product_category_joined_df = product_category_joined_df.join(
        categories_df, product_category_joined_df.category_id == categories_df.category_id, "left"
    ).drop(categories_df.category_id)  # чтобы не было дублирования столбца category_id

    # Выбираем только нужные столбцы
    product_category_pairs_df = product_category_joined_df.select("product_name", "category_name")

    # 2. Определяем продукты без категорий
    # Находим продукты, которые не встречаются в product_categories_df
    products_with_categories = product_categories_df.select("product_id").distinct()

    unmatched_products_df = products_df.join(
        products_with_categories, products_df.product_id == products_with_categories.product_id, "left_anti"
    ).select("product_name").withColumn("unmatched_product", lit(True)) # Добавляем столбец с флагом, что это продукт без категории

    # 3. Объединяем результаты
    # Для продуктов с категориями столбец "unmatched_product" будет False
    result_df = product_category_pairs_df.withColumn("unmatched_product", lit(False)).unionByName(unmatched_products_df, allowMissingColumns=True)

    return result_df

if __name__ == '__main__':
    # Пример использования
    spark = SparkSession.builder.appName("ProductCategoryPairs").getOrCreate()
    products_data = [
        (1, "Яблоко"),
        (2, "Пицца"),
        (3, "Кроссовки"),
        (4, "Тетрадь")  # Пример продукта без категории
    ]
    products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])

    categories_data = [
        (101, "Фрукты"),
        (102, "Еда"),
        (103, "Обувь")
    ]
    categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name"])

    product_categories_data = [
        (1, 101), # Яблоко - Фрукты
        (1, 102), # Яблоко - Еда
        (2, 102), # Пицца - Еда
        (3, 103), # Кроссовки - Обувь
    ]
    product_categories_df = spark.createDataFrame(product_categories_data, ["product_id", "category_id"])


    # Вызываем функцию
    result_df = get_product_category_pairs_and_unmatched_products(products_df, categories_df, product_categories_df)

    # Выводим результат
    result_df.show(truncate=False)

    spark.stop()
