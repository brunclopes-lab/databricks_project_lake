# Databricks notebook source
from faker import Faker 
from faker.providers import DynamicProvider
from datetime import date

fake = Faker()

# COMMAND ----------

# MAGIC %run ../functions/functions

# COMMAND ----------

path_folder = 'faker'
folder_name = "produtos"
mode = "overwrite"

path = f"{path_folder}/{folder_name}"

# COMMAND ----------

produtos = DynamicProvider(
    provider_name="product_name",
    elements=[
    "Bread", "Milk", "Eggs", "Cheese", "Yogurt", "Chicken", "Beef", "Pork", "Fish", "Rice",
    "Pasta", "Tomatoes", "Lettuce", "Potatoes", "Onions", "Carrots", "Apples", "Bananas",
    "Oranges", "Strawberries", "Grapes", "Cereal", "Peanut Butter", "Jelly", "Jam", "Honey",
    "Maple Syrup", "Pancake Mix", "Waffles", "Oatmeal", "Soup", "Canned Vegetables",
    "Frozen Vegetables", "Frozen Pizza", "Ice Cream", "Frozen Dinners", "Chips", "Pretzels",
    "Crackers", "Salsa", "Guacamole", "Tortillas", "Hot Sauce", "Ketchup", "Mustard",
    "Mayonnaise", "Pickles", "Salad Dressing", "Olive Oil", "Vinegar", "Salt", "Pepper",
    "Spices", "Herbs", "Sugar", "Flour", "Baking Powder", "Baking Soda", "Chocolate Chips",
    "Nuts", "Dried Fruits", "Coffee", "Tea", "Juice", "Soda", "Water", "Sports Drinks",
    "Energy Drinks", "Wine", "Beer", "Spirits", "Cheese Puffs", "Popcorn", "Candy",
    "Chocolate Bars", "Cookies", "Brownies", "Cake Mix", "Frosting", "Pies", "Pudding",
    "Jello", "Macaroni and Cheese", "Granola Bars", "Protein Bars", "Almond Butter",
    "Quinoa", "Couscous", "Hummus", "Tofu", "Tempeh", "Seitan", "Almond Milk",
    "Coconut Milk", "Soy Milk", "Greek Yogurt", "Cottage Cheese", "Veggie Burgers",
    "Sausages", "Jerky"
]
)

categorias = DynamicProvider(
    provider_name="category_name",
    elements=[
    "Bakery", "Dairy", "Meat", "Seafood", "Grains", "Produce", "Fruits", "Breakfast",
    "Condiments", "Frozen", "Canned Goods", "Snacks", "Spices", "Baking", "Beverages",
    "Alcohol", "Sweets", "Pasta", "Meat Alternatives", "Dairy Alternatives"
]
)

# COMMAND ----------

fake.add_provider(produtos)
fake.add_provider(categorias)

# COMMAND ----------

produtos = []

for i in range(1, 101):
    produto = {
         'id_produto': i
        ,'nome_produto': fake.unique.product_name()
        ,'categoria_produto': fake.category_name()
        ,'data_cadastro': fake.date_time_between(start_date=date(2020, 1, 1), end_date=date(2022, 1, 1)).strftime('%Y-%m-%d')
    }

    produtos.append(produto)

# COMMAND ----------

df_produtos = spark.createDataFrame(produtos)

# COMMAND ----------

write_json_transient(
    df=df_produtos, 
    folder_name=path,
    mode=mode
)
