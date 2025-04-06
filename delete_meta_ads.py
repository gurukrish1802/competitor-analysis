import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
import traceback

# MongoDB connection settings
MONGO_URI = "mongodb+srv://hawkyai:1R9hdMHgV7OsdueE@cluster0.76m5x.mongodb.net/"
DATABASE_NAME = "competitor_analysis_test"
COLLECTION_NAME = "Meta_ads"
PROCESSED_NAME = "processed_creatives"


async def delete_data_by_page_id(page_id):
    """
    Delete all meta ads and processed creatives data for a specific page ID
    """
    try:
        print(f"\n=== Starting Data Deletion ===")
        print(f"Page ID: {page_id}")
        
        # Connect to MongoDB
        client = AsyncIOMotorClient(MONGO_URI)
        db = client[DATABASE_NAME]
        meta_collection = db[COLLECTION_NAME]
        processed_collection = db[PROCESSED_NAME]
        
        results = {}
        
        # Delete meta ads
        meta_count = await meta_collection.count_documents({"page_id": page_id})
        
        if meta_count == 0:
            print(f"❌ No meta ads found for page ID: {page_id}")
            results["meta_ads"] = {"success": False, "message": "No meta ads found"}
        else:
            print(f"Found {meta_count} meta ads documents to delete")
            meta_result = await meta_collection.delete_many({"page_id": page_id})
            
            if meta_result.deleted_count > 0:
                print(f"✅ Successfully deleted {meta_result.deleted_count} meta ads for page ID: {page_id}")
                results["meta_ads"] = {"success": True, "deleted_count": meta_result.deleted_count}
            else:
                print(f"❌ Failed to delete meta ads for page ID: {page_id}")
                results["meta_ads"] = {"success": False, "message": "No documents were deleted"}
        
        # Delete processed creatives
        processed_count = await processed_collection.count_documents({"page_id": page_id})
        
        if processed_count == 0:
            print(f"❌ No processed creatives found for page ID: {page_id}")
            results["processed_creatives"] = {"success": False, "message": "No processed creatives found"}
        else:
            print(f"Found {processed_count} processed creatives documents to delete")
            processed_result = await processed_collection.delete_many({"page_id": page_id})
            
            if processed_result.deleted_count > 0:
                print(f"✅ Successfully deleted {processed_result.deleted_count} processed creatives for page ID: {page_id}")
                results["processed_creatives"] = {"success": True, "deleted_count": processed_result.deleted_count}
            else:
                print(f"❌ Failed to delete processed creatives for page ID: {page_id}")
                results["processed_creatives"] = {"success": False, "message": "No documents were deleted"}
        
        return results
            
    except Exception as e:
        print(f"❌ Error deleting data: {str(e)}")
        traceback.print_exc()
        return {"success": False, "error": str(e)}
    finally:
        client.close()

def main():
    try:
        print("\n=== Data Deletion Script ===")
        
        # Get page ID from user input
        page_id = "229788811057521"
        
        if not page_id:
            print("❌ Page ID is required")
            return
            
        # Run the deletion process
        results = asyncio.run(delete_data_by_page_id(page_id))
        
        if results.get("meta_ads", {}).get("success") or results.get("processed_creatives", {}).get("success"):
            print("\n✅ Deletion completed successfully")
            
            if results.get("meta_ads", {}).get("success"):
                print(f"- Deleted {results['meta_ads']['deleted_count']} meta ads")
            
            if results.get("processed_creatives", {}).get("success"):
                print(f"- Deleted {results['processed_creatives']['deleted_count']} processed creatives")
        else:
            print("\n❌ Deletion failed: No documents were deleted")
            
    except Exception as e:
        print(f"\n❌ Fatal error: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    main() 