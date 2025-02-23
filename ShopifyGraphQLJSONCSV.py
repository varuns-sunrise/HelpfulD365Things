import requests
import json
import pandas as pd

# Shopify API details
SHOPIFY_URL = "https://enkaydev.myshopify.com/admin/api/2024-07/graphql.json"
HEADERS = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": ""
}

# GraphQL Query Template
GRAPHQL_QUERY = """
query Products($cursor: String, $status: String) {
    products(
        query: $status
        first: 250
        after: $cursor
    ) {
        nodes {
            variants(first: 50) {
                nodes {
                    id
                    inventoryItem {
                        id
                    }
                    sku
                }
            }
            id
            metafield(namespace: "custom", key: "jl_cat_code") {
                value
            }
            status
        }
        pageInfo {
            endCursor
            hasNextPage
            hasPreviousPage
            startCursor
        }
    }
}
"""

def fetch_products(status):
    """Fetches all products from Shopify API for a given status."""
    all_products = []
    cursor = None

    while True:
        variables = {
            "cursor": cursor,
            "status": f"status:{status}"
        }

        response = requests.post(SHOPIFY_URL, headers=HEADERS, json={"query": GRAPHQL_QUERY, "variables": variables})
        data = response.json()

        if "data" in data and "products" in data["data"]:
            products = data["data"]["products"]["nodes"]
            all_products.extend(products)

            page_info = data["data"]["products"]["pageInfo"]
            if page_info["hasNextPage"]:
                cursor = page_info["endCursor"]
            else:
                break
        else:
            print("Error fetching data:", data)
            break

    return all_products

# Fetch draft and active products
print("Fetching draft products...")
draft_products = fetch_products("draft")

print("Fetching active products...")
active_products = fetch_products("active")

# Save data to JSON files
with open("draft_products.json", "w") as f:
    json.dump(draft_products, f, indent=4)

with open("active_products.json", "w") as f:
    json.dump(active_products, f, indent=4)

print("Data fetched and saved successfully!")

# ---- Process and Convert to CSV ----

def extract_all_data(file_paths):
    """Extracts relevant product data from JSON files."""
    rows = []
    for file_path in file_paths:
        with open(file_path, "r") as file:
            products = json.load(file)
            for product in products:
                master_id = str(product["id"].split("/")[-1])
                metafield = product.get("metafield")
                catcode = ""
                if metafield and isinstance(metafield, dict) and "value" in metafield:
                    catcode = metafield["value"].replace("|", ",").replace(" ", "")
                for variant in product["variants"]["nodes"]:
                    variant_id = str(variant["id"].split("/")[-1])
                    inventory_item_id = str(variant["inventoryItem"]["id"].split("/")[-1])
                    sku = variant["sku"]
                    rows.append({
                        "Master ID": master_id,
                        "Variant ID": variant_id,
                        "Inventory Item ID": inventory_item_id,
                        "SKU": sku if sku else "",
                        "CatCode": catcode
                    })
    return rows

# Process the JSON files
print("Processing product data...")
combined_rows = extract_all_data(["draft_products.json", "active_products.json"])

# Save to CSV
df_final_combined = pd.DataFrame(combined_rows)
df_final_combined.to_csv("EnkayProducts.csv", index=False)

print("CSV file 'EnkayProducts.csv' created successfully!")
