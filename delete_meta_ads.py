import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
import traceback

# MongoDB connection settings
MONGO_URI = "mongodb+srv://hawkyai:1R9hdMHgV7OsdueE@cluster0.76m5x.mongodb.net/"
DATABASE_NAME = "competitor_analysis_test"
COLLECTION_NAME = "Meta_ads"


async def delete_meta_ads(page_id):
    """
    Delete all meta ads data for a specific page ID
    """
    try:
        print(f"\n=== Starting Meta Ads Deletion ===")
        print(f"Page ID: {page_id}")
        
        # Connect to MongoDB
        client = AsyncIOMotorClient(MONGO_URI)
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]
        
        # First, count how many documents match the page_id
        count = await collection.count_documents({"page_id": page_id})
        
        if count == 0:
            print(f"❌ No meta ads found for page ID: {page_id}")
            return {"success": False, "message": "No meta ads found"}
            
        print(f"Found {count} documents to delete")
        
        # Delete all documents with the given page_id
        result = await collection.delete_many({"page_id": page_id})
        
        if result.deleted_count > 0:
            print(f"✅ Successfully deleted all meta ads for page ID: {page_id}")
            print(f"Number of documents deleted: {result.deleted_count}")
            return {"success": True, "deleted_count": result.deleted_count}
        else:
            print(f"❌ Failed to delete meta ads for page ID: {page_id}")
            return {"success": False, "message": "No documents were deleted"}
            
    except Exception as e:
        print(f"❌ Error deleting meta ads: {str(e)}")
        traceback.print_exc()
        return {"success": False, "error": str(e)}
    finally:
        client.close()

def main():
    try:
        print("\n=== Meta Ads Deletion Script ===")
        
        # Get page ID from user input
        page_id = "106729994530632"
        
        if not page_id:
            print("❌ Page ID is required")
            return
            
        # Run the deletion process
        result = asyncio.run(delete_meta_ads(page_id))
        
        if result.get("success"):
            print("\n✅ Deletion completed successfully")
        else:
            print(f"\n❌ Deletion failed: {result.get('message', result.get('error', 'Unknown error'))}")
            
    except Exception as e:
        print(f"\n❌ Fatal error: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    main() 