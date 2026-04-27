import pymongo
from src.config import MONGO_URI, DB_NAME


def get_db_client():
    client = pymongo.MongoClient(MONGO_URI)
    return client


def get_database():
    client = get_db_client()
    return client[DB_NAME]


def get_product_targets(db):
    product_col = db["product_dictionary"]
    summary_col = db["summary"]

    if product_col.count_documents({}) > 0:
        query = {
            "status": {
                "$not": {"$regex": "Success|404|Not found", "$options": "i"}
            }
        }

        cursor = product_col.find(query)
        product_targets = [{"product_id": doc["product_id"], "url": doc["url"]} for doc in cursor]

        return product_targets

    group_1_events = [
        "view_product_detail", "select_product_option", "select_product_option_quality",
        "add_to_cart_action", "product_detail_recommendation_visible", "product_detail_recommendation_noticed"
    ]
    group_2_events = ["product_view_all_recommend_clicked"]

    pipeline = [
        {"$match": {"collection": {"$in": group_1_events + group_2_events}}},
        {"$addFields": {
            "target_id": {
                "$cond": [
                    {"$in": ["$collection", group_1_events]},
                    {"$ifNull": ["$product_id", "$viewing_product_id"]},
                    "$viewing_product_id"
                ]
            },
            "target_url": {
                "$cond": [
                    {"$in": ["$collection", group_1_events]},
                    "$current_url",
                    "$referrer_url"
                ]
            }
        }},
        {"$match": {
            "target_id": {"$nin": [None, ""]},
            "target_url": {"$nin": [None, ""]}
        }},
        {"$group": {
            "_id": "$target_id",
            "url": {"$first": "$target_url"}
        }}
    ]

    cursor = summary_col.aggregate(pipeline, allowDiskUse=True)
    product_targets = [{"product_id": doc["_id"], "url": doc["url"]} for doc in cursor]

    return product_targets