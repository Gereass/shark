from pyspark.sql.functions import col

def get_product_category_pairs(df):
    product_category_pairs = df.select("product_name", "category_name")
    
    products_with_no_category = df.filter(col("category_name").isNull()).select("product_name")
    
    return product_category_pairs, products_with_no_category
