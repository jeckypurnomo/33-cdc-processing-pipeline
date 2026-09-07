from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark Session
spark = (
    SparkSession.builder
    .appName("CDCProcessingPipeline")
    .master("local[*]")
    .getOrCreate()
)


# ==== EXTRACT ====


#Read The Dataset
orders_existing_df = (
    spark.read.csv(
        "data/orders_existing.csv",
        header=True,
        inferSchema=True
    )
)

orders_cdc_df = (
    spark.read.csv(
        "data/orders_cdc.csv",
        header=True,
        inferSchema=True
    )
)

# Display the Dataset
print("\n--- Orders Existing Dataset ---")
orders_existing_df.show()

print("\n--- Orders CDC Dataset ---")
orders_cdc_df.show()

# Display Dataset Schema 
print("\n--- Orders Existing Schema ---")
orders_existing_df.printSchema()

print("\n--- Orders CDC Schema ---")
orders_cdc_df.printSchema()


# ==== TRANSFORM ====


valid_operations = ["INSERT", "UPDATE", "DELETE"]

invalid_operations_df = (
    orders_cdc_df
    .filter(~col("operation").isin(valid_operations))
)

invalid_count = invalid_operations_df.count()

if invalid_count > 0:
    print("\nInvalid CDC operations found:")
    invalid_operations_df.show()
    raise ValueError("CDC validation failed.")

print("\nCDC operation validation passed.")

# Filter Orders CDC 
orders_cdc_updates_df = (
    orders_cdc_df
    .filter(
        col("operation") == "UPDATE"
    )
)

orders_cdc_delete_df = (
    orders_cdc_df
    .filter(
        col("operation") == "DELETE"
    )
)

orders_cdc_insert_df = (
    orders_cdc_df
    .filter(
        col("operation") == "INSERT"
    )
)

# Display Filtered Orders CDC 
print("\n--- Update Orders CDC ---")
orders_cdc_updates_df.show()

print("\n--- Delete Orders CDC ---")
orders_cdc_delete_df.show()

print("\n--- Insert Orders CDC ---")
orders_cdc_insert_df.show()

# Unchanged Dataset
unchanged_df = (
    orders_existing_df
    .join(
        orders_cdc_delete_df,
        on="order_id",
        how="left_anti"
    )
    .join(
        orders_cdc_updates_df,
        on="order_id",
        how="left_anti"
    )
)

# Display Unchanged Dataset
print("\n--- Unchanged Dataset ---")
unchanged_df.show()

# Prepare Data For Upsert
cdc_changes_df = (
    orders_cdc_updates_df
    .unionByName(
        orders_cdc_insert_df
    )
    .drop(
        col("operation")
    )
)

# Display Upsert Dataset
print("\n--- CDC Changes Dataset ---")
cdc_changes_df.show()

# Create Latest Orders 
latest_orders = (
    unchanged_df
    .unionByName(
        cdc_changes_df
    )
    .orderBy(
        col("order_id")
    )
)

# Display Latest Orders
print("\n--- Latest Orders ---")
latest_orders.show()

insert_count = orders_cdc_insert_df.count()
update_count = orders_cdc_updates_df.count()
delete_count = orders_cdc_delete_df.count()
final_order_count = latest_orders.count()

print("\n" + "=" * 40)
print("CDC Processing Pipeline Completed")
print("=" * 40)
print("\nINSERT Count         :", insert_count)
print("UPDATE Count         :", update_count)
print("DELETE Count         :", delete_count)
print("Final Orders Count   :", final_order_count)


# ==== LOAD ====


latest_orders.write \
    .mode("overwrite") \
    .parquet("output/latest_orders/")

print("\nLatest Orders Saved Successfully.")

spark.stop()