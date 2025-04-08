import asyncio
import hashlib
import json
import os
from datetime import datetime, timedelta
import traceback
from contextlib import asynccontextmanager
import requests
import aiofiles
import aiohttp
from elasticsearch import AsyncElasticsearch
from motor.motor_asyncio import AsyncIOMotorClient
import google.generativeai as genai
from bson import ObjectId
from time import time
from brand_analyser import BrandAnalyzer  # Import the BrandAnalyzer class
import random
from flask import Flask, request, jsonify
from dotenv import load_dotenv
import threading
from celery import Celery
import redis

# Load environment variables
load_dotenv()

# Configuration constants
MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://hawkyai:1R9hdMHgV7OsdueE@cluster0.76m5x.mongodb.net/")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "AIzaSyAGRJYtNIXE__r5GzAtziZsftbDDeQWoIo")
genai.configure(api_key=GEMINI_API_KEY)

# Base Redis URL without database number
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
REDIS_BASE_URL = REDIS_URL.split('?')[0]  # Strip query parameters for base URL

# Database settings
database_name = os.getenv("DATABASE_NAME", "competitor_analysis_test")

# Elasticsearch settings
ELASTICSEARCH_CLOUD_ID = os.getenv("ELASTICSEARCH_CLOUD_ID", "h-staging-ES:Y2VudHJhbGluZGlhLmF6dXJlLmVsYXN0aWMtY2xvdWQuY29tOjkyNDMkYjA5NWRmNDQ1OWQxNGUwMWI3MjMwMDU3ZjkwNTNhZTYkMmYzNjJkZTkyNWRjNDAzMGJiODU5NzFmNGFkZDA0ODQ=")
ELASTICSEARCH_API_KEY = os.getenv("ELASTICSEARCH_API_KEY", "dlJLNlBaVUJGZ2lrTUZ4S0FnODY6OEl2MDNqME1UZUNKZ2xYdWFlUzd3dw==")
ELASTICSEARCH_IMAGE_INDEX = os.getenv("ELASTICSEARCH_IMAGE_INDEX", "image_competitors_analysis")
ELASTICSEARCH_VIDEO_INDEX = os.getenv("ELASTICSEARCH_VIDEO_INDEX", "video_competitors_analysis")

# Video Analysis Response Schema for Gemini
VIDEO_RESPONSE_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "hook": {"type": "STRING"},
        "advertisement_type": {"type": "STRING"},
        "motto": {"type": "STRING"},
        "title": {"type": "STRING"},
        "keywords": {
            "type": "ARRAY",
            "items": {
                "type": "OBJECT",
                "properties": {
                    "keyword": {"type": "STRING"},
                    "weight": {"type": "NUMBER"}
                }
            }
        },
        "theme": {"type": "STRING"},
        "key_moments": {
            "type": "ARRAY",
            "items": {
                "type": "OBJECT",
                "properties": {
                    "timestamp": {"type": "STRING"},
                    "description": {"type": "STRING"}
                }
            }
        },
        "summary": {"type": "STRING"},
        "target_audience": {"type": "STRING"},
        "call_to_action": {"type": "STRING"},
        "tone_and_style": {
            "type": "OBJECT",
            "properties": {
                "tone": {"type": "STRING"},
                "writing_style": {"type": "STRING"},
                "visual_style": {"type": "STRING"}
            }
        },
        "visual_focus": {
            "type": "OBJECT",
            "properties": {
                "main_element": {"type": "STRING"},
                "focus_area": {"type": "STRING"},
                "visual_hierarchy": {"type": "STRING"}
            }
        },
        "h1_styles": {
            "type": "OBJECT",
            "properties": {
                "font_style": {"type": "STRING"},
                "color": {"type": "STRING"},
                "formatting": {"type": "ARRAY", "items": {"type": "STRING"}},
                "position": {"type": "STRING"}
            }
        },
        "unique_selling_proposition": {"type": "STRING"},
        "main_topic": {"type": "STRING"},
        "objective": {"type": "STRING"},
        "description": {
            "type": "OBJECT",
            "properties": {
                "subjects": {
                    "type": "ARRAY",
                    "items": {"type": "STRING"}
                },
                "colour_schema": {
                    "type": "ARRAY",
                    "items": {"type": "STRING"}
                },
                "emotional_appeal": {
                    "type": "ARRAY",
                    "items": {"type": "STRING"}
                }
            }
        },
        "narrative_analysis": {
            "type": "OBJECT",
            "properties": {
                "story_flow": {
                    "type": "ARRAY",
                    "items": {"type": "STRING"}
                },
                "message_progression": {
                    "type": "ARRAY",
                    "items": {"type": "STRING"}
                },
                "key_moments": {
                    "type": "ARRAY",
                    "items": {"type": "STRING"}
                }
            }
        },
        "user_journey": {
            "type": "OBJECT",
            "properties": {
                "problem_framing": {"type": "STRING"},
                "solution_presentation": {"type": "STRING"},
                "benefit_demonstration": {
                    "type": "ARRAY",
                    "items": {"type": "STRING"}
                }
            }
        },
        "conversion_path": {
            "type": "OBJECT",
            "properties": {
                "cta_timing": {
                    "type": "ARRAY",
                    "items": {"type": "STRING"}
                }
            }
        },
        "product_categories": {
            "type": "ARRAY",
            "items": {"type": "STRING"}
        },
        "product_features": {
            "type": "ARRAY",
            "items": {"type": "STRING"}
        },
        "messaging_angle": {
            "type": "OBJECT",
            "properties": {
                "primary_approach": {"type": "STRING"},
                "message_framing": {"type": "STRING"},
                "messaging_tactics": {
                    "type": "ARRAY",
                    "items": {"type": "STRING"}
                },
                "notable_aspects": {"type": "STRING"}
            }
        }
    }
}

# Create Flask app
app = Flask(__name__)

# Test broker connection (DB 0)
broker_url = f"{REDIS_URL}"
try:
    redis_client = redis.Redis.from_url(broker_url)
    redis_client.ping()
    print("Redis broker connection successful!")
except Exception as e:
    print(f"Redis broker connection failed: {e}")

# Test backend connection (DB 1)
backend_url = f"{REDIS_BASE_URL}/1{'?' + REDIS_URL.split('?')[1] if '?' in REDIS_URL else ''}"
try:
    redis_client = redis.Redis.from_url(backend_url)
    redis_client.ping()
    print("Redis backend connection successful!")
except Exception as e:
    print(f"Redis backend connection failed: {e}")

# Configure Celery
celery_app = Celery(
    'creative_extraction',
    broker=f"{REDIS_BASE_URL}/0{'?' + REDIS_URL.split('?')[1] if '?' in REDIS_URL else ''}",  # Database 0 for broker
    backend=f"{REDIS_BASE_URL}/1{'?' + REDIS_URL.split('?')[1] if '?' in REDIS_URL else ''}"  # Database 1 for backend
)

# Celery configuration (unchanged)
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=3600,  # 1-hour timeout
    worker_max_tasks_per_child=200
)

# Helper functions
async def add_data_to_mongodb(collection_name, document):
    """Add document to MongoDB collection"""
    try:
        client = AsyncIOMotorClient(MONGO_URI)
        db = client[database_name]
        collection = db[collection_name]
        result = await collection.insert_one(document)
        print(f"Data inserted with ID {result.inserted_id}")
        return result.inserted_id
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        client.close()

def download_video_chunk(url, chunk_size=512 * 512):
    response = requests.get(url, stream=True)
    response.raise_for_status()
    hash_func = hashlib.sha256()
    downloaded = 0
    for chunk in response.iter_content(chunk_size=8192):
        hash_func.update(chunk)
        downloaded += len(chunk)
        if downloaded >= chunk_size:
            break
    return hash_func.hexdigest()

def download_image_for_hash(url):
    response = requests.get(url, stream=True)
    response.raise_for_status()
    hash_func = hashlib.sha256()
    for chunk in response.iter_content(chunk_size=8192):
        hash_func.update(chunk)
    return hash_func.hexdigest()

class CreativeAnalysis:
    def __init__(self, brand_id, brand_url=None):
        self.brand_id = brand_id
        self.brand_url = brand_url
        self.mongo_client = AsyncIOMotorClient(MONGO_URI)
        self.db = self.mongo_client[database_name]
        self.test_db = self.mongo_client['test']  # Add test database connection
        self.http_session = None
        self.es_client = None
        self.currently_processing = set()
        self.processing_lock = asyncio.Lock()
        self.es_image_batch = []
        self.es_video_batch = []
        self.mongodb_batch = []
        self.batch_size = 100
        self.api_keys = [
            "AIzaSyBmkinLZGJ7iE6cPMNjYKpbMUtoaJyakGc",
            "AIzaSyD5AR-e6HiaCO1Qa7JhRT9n2l91a-KP2BM",
            "AIzaSyAGRJYtNIXE__r5GzAtziZsftbDDeQWoIo",
            "AIzaSyA8UhjLDFw0uFyujJlbW18vdVmI9_9nuFE"
        ]
        self.key_counters = {key: 0 for key in self.api_keys}
        self.key_timeouts = {}
        self.brand_analyzer = BrandAnalyzer()

    async def initialize(self):
        try:
            self.http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10000))
            # Create index on hash and page_id to improve query performance
            # But make only hash unique since the same page_id can have multiple hashes
            await self.db.processed_creatives.create_index([("hash", 1)], unique=True)
            await self.db.processed_creatives.create_index([("page_id", 1)])
            
            self.es_enabled = False
            try:
                print("Initializing Elasticsearch connection...")
                self.es_client = AsyncElasticsearch(
                    cloud_id=ELASTICSEARCH_CLOUD_ID,
                    api_key=ELASTICSEARCH_API_KEY,
                    verify_certs=True,
                    timeout=30,
                    max_retries=3
                )
                
                # Test the connection
                info = await self.es_client.info()
                print(f"✅ Elasticsearch connected successfully")
                print(f"ES Version: {info.get('version', {}).get('number', 'unknown')}")
                print(f"Cluster Name: {info.get('cluster_name', 'unknown')}")
                
                # Verify we can perform operations
                test_index = "test_connection"
                try:
                    await self.es_client.indices.create(index=test_index, ignore=400)
                    await self.es_client.indices.delete(index=test_index, ignore=404)
                    print("✅ Elasticsearch operations verified")
                    self.es_enabled = True
                except Exception as op_error:
                    print(f"❌ Elasticsearch operations failed: {str(op_error)}")
                    self.es_client = None
                    self.es_enabled = False
                    
            except Exception as es_error:
                print(f"❌ Elasticsearch connection failed: {str(es_error)}")
                self.es_client = None
                self.es_enabled = False

            if self.es_enabled:
                await self.ensure_es_indexes()
            
            # Extract brand name from URL if available
            brand_name = "Unknown Brand"
            if self.brand_url:
                try:
                    # Remove http:// or https:// and get domain
                    domain = self.brand_url.split('//')[1].split('.')[0]
                    # Skip 'www' if it's the first part of the domain
                    if domain.lower() == 'www':
                        # Get the actual domain name after www
                        domain = self.brand_url.split('//')[1].split('.')[1]
                    # Convert domain to title case and replace hyphens/underscores with spaces
                    brand_name = domain.replace('-', ' ').replace('_', ' ').title()
                    print(f"✅ Extracted brand name from URL: {brand_name}")
                except Exception as e:
                    print(f"⚠️ Could not extract brand name from URL: {str(e)}")
            
            # Initialize default brand details with extracted brand name
            self.brand_details = {
                "brand_id": self.brand_id,
                "core_identity": {
                    "brand_name": brand_name,
                    "brand_vision": "Not available",
                    "brand_mission": "Not available",
                    "core_values": ["Not available"]
                },
                "brand_as_product": {
                    "product_category": "Not available",
                    "quality_position": "Not available",
                    "product_categories": []
                },
                "brand_as_person": {
                    "brand_personality": ["Not available"]
                },
                "brand_as_symbol": {
                    "visual_style": ["Not available"]
                },
                "product_categories": []  # Changed from 'products' to 'product_categories'
            }
            
            # Perform full brand analysis if URL is provided
            if self.brand_url:
                await self.analyze_brand()
                print("✅ Brand analysis completed")
            else:
                print("⚠️ No brand URL provided, using default brand details")
                
        except Exception as e:
            print(f"Error initializing: {str(e)}")
            raise

    async def ensure_es_indexes(self):
        try:
            image_index_mapping = {
                "mappings": {
                    "properties": {
                        "url": {"type": "keyword"},
                        "ad_id": {"type": "keyword"},
                        "media_type": {"type": "keyword"},
                        "time": {"type": "date"},
                        "hash": {"type": "keyword"},
                        "page_id": {"type": "keyword"},
                        "status": {"type": "keyword"},
                        "start_date": {"type": "date"},
                        "H1": {"type": "text"},
                        "H1_styles": {
                            "properties": {
                                "font_style": {"type": "text"},
                                "color": {"type": "keyword"},
                                "formatting": {"type": "keyword"},
                                "position": {"type": "keyword"}
                            }
                        },
                        "H2": {"type": "text"},
                        "H3": {"type": "text"},
                        "Objective": {"type": "text"},
                        "Subjects": {"type": "keyword"},
                        "Colour_schema": {"type": "keyword"},
                        "Emotional_appeal": {"type": "keyword"},
                        "brand_colors": {"type": "keyword"},
                        "usps": {"type": "text"},
                        "hook": {"type": "text"},
                        "title": {"type": "text"},
                        "theme": {"type": "text"},
                        "keywords": {"type": "keyword"},
                        "tone_and_style": {
                            "properties": {
                                "tone": {"type": "keyword"},
                                "writing_style": {"type": "keyword"},
                                "visual_style": {"type": "keyword"}
                            }
                        },
                        "visual_focus": {
                            "properties": {
                                "main_element": {"type": "text"},
                                "focus_area": {"type": "keyword"},
                                "visual_hierarchy": {"type": "text"}
                            }
                        },
                        "conversion_elements": {
                            "properties": {
                                "cta": {"type": "text"},
                                "cta_type": {"type": "keyword"},
                                "cta_placement": {"type": "keyword"},
                                "supporting_elements": {"type": "keyword"}
                            }
                        },
                        "audience_targeting": {
                            "properties": {
                                "primary_audience": {"type": "text"},
                                "demographic_indicators": {"type": "keyword"}
                            }
                        }
                    }
                }
            }

            video_index_mapping = {
                "mappings": {
                    "properties": {
                        "url": {"type": "keyword"},
                        "ad_id": {"type": "keyword"},
                        "media_type": {"type": "keyword"},
                        "time": {"type": "date"},
                        "hash": {"type": "keyword"},
                        "page_id": {"type": "keyword"},
                        "status": {"type": "keyword"},
                        "start_date": {"type": "date"},
                        "hook": {"type": "text"},
                        "advertisement_type": {"type": "keyword"},
                        "motto": {"type": "text"},
                        "title": {"type": "text"},
                        "keywords": {
                            "type": "nested",
                            "properties": {
                                "keyword": {"type": "keyword"},
                                "weight": {"type": "float"}
                            }
                        },
                        "theme": {"type": "text"},
                        "summary": {"type": "text"},
                        "target_audience": {"type": "text"},
                        "call_to_action": {"type": "text"},
                        "tone_and_style": {
                            "properties": {
                                "tone": {"type": "keyword"},
                                "writing_style": {"type": "keyword"},
                                "visual_style": {"type": "keyword"}
                            }
                        },
                        "visual_focus": {
                            "properties": {
                                "main_element": {"type": "text"},
                                "focus_area": {"type": "keyword"},
                                "visual_hierarchy": {"type": "text"}
                            }
                        },
                        "h1_styles": {
                            "properties": {
                                "font_style": {"type": "text"},
                                "color": {"type": "keyword"},
                                "formatting": {"type": "keyword"},
                                "position": {"type": "keyword"}
                            }
                        },
                        "unique_selling_proposition": {"type": "text"},
                        "main_topic": {"type": "text"},
                        "objective": {"type": "text"},
                        "description": {
                            "properties": {
                                "subjects": {"type": "keyword"},
                                "colour_schema": {"type": "keyword"},
                                "emotional_appeal": {"type": "keyword"}
                            }
                        },
                        "narrative_analysis": {
                            "properties": {
                                "story_flow": {"type": "keyword"},
                                "message_progression": {"type": "keyword"},
                                "key_moments": {"type": "keyword"}
                            }
                        },
                        "user_journey": {
                            "properties": {
                                "problem_framing": {"type": "text"},
                                "solution_presentation": {"type": "text"},
                                "benefit_demonstration": {"type": "keyword"}
                            }
                        },
                        "conversion_path": {
                            "properties": {
                                "cta_timing": {"type": "keyword"}
                            }
                        },
                        "key_moments": {
                            "type": "nested",
                            "properties": {
                                "timestamp": {"type": "keyword"},
                                "description": {"type": "text"}
                            }
                        },
                        "product_categories": {"type": "keyword"},
                        "product_features": {"type": "keyword"}
                    }
                }
            }

            # Check and create image index if needed
            if not await self.es_client.indices.exists(index=ELASTICSEARCH_IMAGE_INDEX):
                await self.es_client.indices.create(
                    index=ELASTICSEARCH_IMAGE_INDEX,
                    body=image_index_mapping
                )
                print(f"Created Elasticsearch index: {ELASTICSEARCH_IMAGE_INDEX}")
            else:
                print(f"Using existing Elasticsearch index: {ELASTICSEARCH_IMAGE_INDEX}")
            
            # Check and create video index if needed
            if not await self.es_client.indices.exists(index=ELASTICSEARCH_VIDEO_INDEX):
                await self.es_client.indices.create(
                    index=ELASTICSEARCH_VIDEO_INDEX,
                    body=video_index_mapping
                )
                print(f"Created Elasticsearch index: {ELASTICSEARCH_VIDEO_INDEX}")
            else:
                print(f"Using existing Elasticsearch index: {ELASTICSEARCH_VIDEO_INDEX}")
            
        except Exception as e:
            print(f"Error creating Elasticsearch indexes: {str(e)}")
            traceback.print_exc()
            raise

    def convert_objectid_to_str(self, obj):
        if isinstance(obj, dict):
            return {key: self.convert_objectid_to_str(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self.convert_objectid_to_str(item) for item in obj]
        elif isinstance(obj, ObjectId):
            return str(obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        return obj

    async def cleanup(self):
        try:
            if self.http_session and not self.http_session.closed:
                await self.http_session.close()
            if self.es_client:
                await self.es_client.close()
        except Exception as e:
            print(f"Error closing resources: {e}")
        if hasattr(self, 'mongo_client'):
            self.mongo_client.close()

    def extract_media_urls(self, ad):
        """Extract media URLs from the ad structure"""
        media_urls = []
        media_types = []
        try:
            # First try the new structure with media_details
            media_details = ad.get('media_details', [])
            if media_details:
                for media in media_details:
                    media_type = media.get('type')
                    if media_type == 'video':
                        url = media.get('url')
                        if url:
                            media_urls.append(url)
                            media_types.append('video')
                    elif media_type == 'image':
                        url = media.get('image_src')
                        if url:
                            media_urls.append(url)
                            media_types.append('image')
            else:
                # Try the old structure with direct image/video URLs
                if 'image_url' in ad:
                    media_urls.append(ad['image_url'])
                    media_types.append('image')
                if 'video_url' in ad:
                    media_urls.append(ad['video_url'])
                    media_types.append('video')
                
                # Try asset_feed_spec structure
                if 'asset_feed_spec' in ad:
                    asset_feed = ad['asset_feed_spec']
                    if 'images' in asset_feed:
                        for image in asset_feed['images']:
                            if image.get('image_url'):
                                media_urls.append(image['image_url'])
                                media_types.append('image')
                    if 'videos' in asset_feed:
                        for video in asset_feed['videos']:
                            if video.get('video_url'):
                                media_urls.append(video['video_url'])
                                media_types.append('video')
            
            print(f"Found {len(media_urls)} media URLs: {media_urls}")
            print(f"Media types: {media_types}")
            
        except Exception as e:
            print(f"Error extracting media URLs: {str(e)}")
            print(f"Ad data: {json.dumps(ad, indent=2)}")
        return media_urls, media_types

    def create_extraction_prompt(self, url, media_type, brand_details):
        # Get product categories from brand details
        product_categories = brand_details.get('product_categories', [])
        if not product_categories:
            product_categories = brand_details.get('brand_as_product', {}).get('product_categories', [])
        print(f"Available product categories for matching: {product_categories}")

        brand_context = f"""
    Brand Context:
    - Brand Name: {brand_details.get('core_identity', {}).get('brand_name', 'Unknown Brand')}
    - Vision: {brand_details.get('core_identity', {}).get('brand_vision', 'Not available')}
    - Mission: {brand_details.get('core_identity', {}).get('brand_mission', 'Not available')}
    - Core Values: {', '.join(brand_details.get('core_identity', {}).get('core_values', ['Not available']))}
    - Product Category: {brand_details.get('brand_as_product', {}).get('product_category', 'Not available')}
    - Quality Position: {brand_details.get('brand_as_product', {}).get('quality_position', 'Not available')}
    - Brand Personality: {', '.join(brand_details.get('brand_as_person', {}).get('brand_personality', ['Not available']))}
    - Visual Style: {', '.join(brand_details.get('brand_as_symbol', {}).get('visual_style', ['Not available']))}
    
    Available Product Categories:
    {json.dumps(product_categories, indent=2)}
    """

        extraction_instructions = f"""
    You are a Creative Analysis Tool performing entity extraction on a {media_type} creative.
    URL being analyzed: {url}

    Consider the brand context and product categories provided above when analyzing the creative.
    
    IMPORTANT PRODUCT CATEGORY MATCHING RULES:
    1. First try to match products shown in the creative with EXACT category names from the provided list
    2. If no exact match is found, look for RELATED categories from the provided list that best match the products shown
    3. Only include categories from the provided list - do not create new categories
    4. For product features, list all specific features, technologies, or benefits shown or mentioned in the creative

    Task: Extract and analyze the following specific elements from the {media_type}:

    IMPORTANT FORMATTING RULES:
    1. DO NOT use any Unicode characters or special symbols
    2. DO NOT include any escape characters (\n, \t, \r, etc.)
    3. Use only plain ASCII characters
    4. Convert any special characters to their plain text equivalent
    5. Remove any emojis or convert them to text descriptions
    6. Ensure all text is clean and directly usable without any encoding issues
    7. Use simple quotation marks (' or ") instead of curly quotes
    8. Replace any special punctuation with standard ASCII equivalents
    9. Keep responses clear and encoding-friendly

    Elements to extract:

    1. H1:
    - The text with largest font format in the creative
    - Typically the most prominent text element

    2. H1 Styles:
    - Font style, color, and formatting of the H1 text
    - Any special visual treatment (e.g., bold, italic, underlined, boxed)
    - Position and alignment of the H1 text in the creative

    3. H2:
    - Text with medium font size
    - Usually secondary heading or important message

    4. H3:
    - Text with smaller font size
    - Could be supporting information or details

    5. Objective:
    - What is the primary objective of this advertisement?
    - What action does the brand want the viewer to take?

    6. Subjects:
    - Identify all human subjects or objects featured prominently
    - Note any demographic characteristics if humans are present

    7. Colour Schema:
    - Short description of the color palette used
    - Note any dominant or accent colors

    8. Emotional Appeal:
    - What emotions does the creative try to evoke?
    - What emotional triggers are used?

    9. Brand Colors:
    - Specific colors that appear to be part of the brand identity
    - How these align with known brand colors

    10. USPs (Unique Selling Points):
    - Key differentiators or benefits highlighted

    11. Hook:
    - The initial element that grabs the viewer's attention
    - This could be a visual, text, or a combination of both

    12. Conversion Elements:
        - CTA (Call to Action): The specific action text
        - CTA Type: Button, text link, banner, etc.
        - CTA Placement: Location on the creative (top, bottom, left, right, center)
        - Supporting Elements: Features that help drive conversion

    13. Audience Targeting:
        - Primary Audience: Main target demographic
        - Demographic Indicators: Visual cues that suggest target audience

    14. Title:
        - A concise title that captures the main message of the ad
        - Should be descriptive and reflective of the content

    15. Theme:
        - The main theme or message of the ad
        - Overall concept or idea being communicated

    16. Keywords:
        - List of relevant keywords that describe the ad content
        - Include both visual and textual elements

    17. Tone & Style:
        - The overall tone of the creative (e.g., formal, casual, humorous, serious)
        - The writing style and voice used in the text
        - Visual style elements (e.g., minimalist, vibrant, corporate, playful)

    18. Language:
        - Primary language used in the creative
        - Any secondary languages or multilingual elements
        - Language style (formal, informal, technical, colloquial)
        - Cultural language references or idioms

    19. Subject Focus:
        - Who is the primary subject of the creative (person, group, object)
        - Subject's demographic characteristics (age, gender, ethnicity, etc.)
        - Subject's emotional state and expressions
        - Subject's positioning and prominence in the creative
        - How the subject relates to the target audience

    20. Product Focus and Placements:
        - How products are featured in the creative
        - Product placement strategy (central, peripheral, background)
        - Product positioning relative to other elements
        - Product emphasis techniques (size, color, lighting, framing)
        - Product interaction with subjects or environment

    21. Visual Focus:
        - The main visual element that draws attention
        - What the creative primarily focuses on visually
        - Visual hierarchy and how attention is directed through the creative

    22. Product Analysis:
        - Product Categories: Look for any products shown in the creative and match them with these categories:
        {json.dumps(product_categories, indent=2)}
        - Product Features: List all specific features, technologies, or benefits shown or mentioned in the creative
        
    23. Messaging Angle:
        - The primary messaging approach used in the creative (e.g., emotional, rational, fear-based, solution-oriented)
        - How the message is framed (e.g., problem-solution, aspiration, testimonial, storytelling)
        - Specific messaging tactics used (e.g., social proof, scarcity, authority, urgency)
        - Any unique or notable aspects of how the message is delivered

    """

        return_format_instructions = """
    Please return your analysis in the following specific JSON structure:

    {
        "H1": "text with largest font format",
        "H1_styles": {
            "font_style": "description of font style",
            "color": "color of the H1 text",
            "formatting": ["list of formatting treatments"],
            "position": "position in the creative"
        },
        "H2": "text with medium font",
        "H3": "text with small font",
        "Objective": "objective of ad",
        "Subjects": ["list of subjects in ad like human / object etc if available or empty"],
        "Colour_schema": ["short description of colour schema"],
        "Emotional_appeal": ["Emotional appeals"],
        "brand_colors": ["colors used"],
        "usps": "unique selling points mentioned",
        "hook": "hooks in the creative",
        "title": "concise title for the ad",
        "theme": "main theme or message",
        "keywords": ["relevant keywords"],
        "tone_and_style": {
            "tone": "overall tone of the creative",
            "writing_style": "style of writing used",
            "visual_style": "visual style elements"
        },
        "language": {
            "primary": "main language used",
            "secondary": ["any additional languages"],
            "style": "language style description",
            "cultural_references": ["any cultural language elements"]
        },
        "subject_focus": {
            "primary_subject": "main subject description",
            "demographics": ["demographic characteristics"],
            "emotional_state": "subject's emotional expression",
            "positioning": "how subject is positioned",
            "audience_relation": "how subject relates to target audience"
        },
        "product_focus": {
            "featured_products": ["products prominently shown"],
            "placement_strategy": "how products are placed",
            "positioning": "product positioning description",
            "emphasis_techniques": ["techniques used to emphasize products"],
            "interaction": "how products interact with other elements"
        },
        "visual_focus": {
            "main_element": "primary visual element",
            "focus_area": "area of focus",
            "visual_hierarchy": "how visual attention is directed"
        },
        "product_categories": ["ONLY product categories from the provided list that appear in the creative"],
        "product_features": ["specific features of the products shown in the creative"],
        "conversion_elements": {
            "cta": "call to action text",
            "cta_type": "call to action type",
            "cta_placement": "location description",
            "supporting_elements": ["elements supporting conversion"]
        },
        "audience_targeting": {
            "primary_audience": "main target audience",
            "demographic_indicators": ["visual demographic cues"]
        },
        "messaging_angle": {
            "primary_approach": "primary messaging approach used",
            "message_framing": "how the message is framed",
            "messaging_tactics": ["specific messaging tactics used"],
            "notable_aspects": "unique aspects of message delivery"
        }
    }

    Important:
    1. NEVER return null or empty strings
    2. If an element is not explicitly visible, derive it from context
    3. Every field must contain meaningful content
    4. Ensure all arrays have at least one item
    5. For product_categories, ONLY include exact category matches from the provided list
    6. For product_features, extract features directly from the creative content
    """

        return brand_context + extraction_instructions + return_format_instructions

    async def _get_available_key(self):
        best_key = self.api_keys[0]
        best_timeout = float('inf')
        for key in self.api_keys:
            if key in self.key_timeouts:
                current_time = time()
                if current_time < self.key_timeouts[key]:
                    if self.key_timeouts[key] < best_timeout:
                        best_timeout = self.key_timeouts[key]
                        best_key = key
                else:
                    del self.key_timeouts[key]
                    return key
            else:
                return key
        wait_time = best_timeout - time()
        if wait_time > 0:
            print(f"All keys are in timeout. Waiting {wait_time:.1f} seconds for next available key.")
            await asyncio.sleep(wait_time)
            del self.key_timeouts[best_key]
        return best_key

    def _increment_key_counter(self, key):
        self.key_counters[key] += 1
        if self.key_counters[key] >= 15:
            print(f"⏱️ Reached {self.key_counters[key]} requests for key {key[:8]}..., setting 60s timeout")
            self.key_timeouts[key] = time() + 60
            self.key_counters[key] = 0
            return True
        return False

    async def gemini_video_analysis(self, video_url, max_retries=3):
        keys_tried = set()
        for attempt in range(max_retries):
            try:
                current_api_key = await self._get_available_key()
                current_key_index = self.api_keys.index(current_api_key)
                genai.configure(api_key=current_api_key)
                keys_tried.add(current_key_index)
                print(f"Using API key {current_key_index + 1}/{len(self.api_keys)}: {current_api_key[:8]}...")

                timestamp = int(time())
                temp_file_path = f"temp_timestamp_{timestamp}.mp4"
                display_name = os.path.basename(temp_file_path)

                response = await asyncio.to_thread(
                    lambda: requests.get(video_url, timeout=60)
                )
                limit_reached = self._increment_key_counter(current_api_key)
                if limit_reached:
                    continue
                response.raise_for_status()
                with open(temp_file_path, "wb") as f:
                    f.write(response.content)
                print(f"✅ Video downloaded successfully as: {temp_file_path}")

                video_file = genai.upload_file(path=temp_file_path, display_name=display_name, resumable=True)
                limit_reached = self._increment_key_counter(current_api_key)
                if limit_reached:
                    continue
                print(f"✅ Upload complete. File URI: {video_file.uri}")

                processing_start_time = time()
                while video_file.state.name == "PROCESSING":
                    print(".", end="", flush=True)
                    video_file = genai.get_file(video_file.name)
                    if time() - processing_start_time > 300:
                        print("\n⚠️ Processing timeout, switching API key.")
                        self.key_timeouts[current_api_key] = time() + 60
                        current_api_key = await self._get_available_key()
                        genai.configure(api_key=current_api_key)
                        print(f"Switched to API key: {current_api_key[:8]}...")
                if video_file.state.name == "FAILED":
                    raise ValueError("❌ Video processing failed.")
                print("\n✅ Video is ready for analysis.")

                video_analysis_prompt = (
                    f"Analyze the following advertisement video and extract ALL these fields (do not skip any field):\n"
                    "- Advertisement Type\n"
                    "- Advertisement Summary\n"
                    "- Call to Action (CTA)\n"
                    "- Conversion Path (CTA Timing)\n"
                    "- Description of Subjects, Colour Schema, and Emotional Appeal\n"
                    "- Hook\n"
                    "- Key Moments with timestamps\n"
                    "- Main Topic\n"
                    "- Motto/Slogan\n"
                    "- Narrative Analysis (Story Flow, Message Progression, Key Moments)\n"
                    "- Objective of Ad\n"
                    "- Target Audience\n"
                    "- Tone and Style (Formal/Informal, Emotional/Logical, Visual Style Elements)\n"
                    "- Visual Focus (Main Visual Elements, Focus Areas, Visual Hierarchy)\n"
                    "- H1 Styles (Font Style, Color, Formatting, Position of Main Text)\n"
                    "- Title (A concise title for the ad)\n"
                    "- Keywords (A list of weighted keywords for a word cloud)\n"
                    "- Theme (The main theme or message of the ad)\n"
                    "- Unique Selling Proposition (USP)\n"
                    "- User Journey (Problem Framing, Solution Presentation, Benefit Demonstration)\n"
                    "- Product Categories (List all products shown in the video)\n"
                    "- Product Features (List all features and benefits shown)\n"
                    "- Messaging Angle (Primary approach, message framing, messaging tactics, notable aspects)\n"
                    "- Language (Primary language, secondary languages, language style, cultural references)\n"
                    "- Subject Focus (Who is the primary subject, their demographics, emotional state, positioning, audience relation)\n"
                    "- Product Focus and Placements (How products are featured, placement strategy, positioning, emphasis techniques, interaction)\n\n"
                    "IMPORTANT: You MUST include both product_categories and product_features arrays in your response.\n"
                    "Respond strictly in JSON format with ALL above fields, with keys arranged in alphabetical order."
                )

                model = genai.GenerativeModel(model_name="models/gemini-1.5-pro-latest")
                limit_reached = self._increment_key_counter(current_api_key)
                if limit_reached:
                    continue
                response = model.generate_content(
                    [video_file, video_analysis_prompt],
                    generation_config={"response_schema": VIDEO_RESPONSE_SCHEMA, "response_mime_type": "application/json"},
                    request_options={"timeout": 200}
                )

                parsed_response = json.loads(response.text)
                
                if os.path.exists(temp_file_path):
                    os.remove(temp_file_path)
                    print(f"Deleted temporary file: {temp_file_path}")
                return parsed_response
            except Exception as e:
                print(f"❌ Error during analysis: {e}")
                if attempt < max_retries - 1:
                    backoff_time = (2 ** attempt) * 60
                    jitter = random.uniform(0, 0.1 * backoff_time)
                    total_backoff = backoff_time + jitter
                    print(f"🕒 Waiting {total_backoff:.2f} seconds before retry...")
                    await asyncio.sleep(total_backoff)
                else:
                    print("❌ All retry attempts failed.")
                    return None

    async def gemini_image_analysis(self, image_url, max_retries=3):
        """Perform image analysis using GPT-4 Vision (4o) with failover between three servers"""
        max_retries = 3
        base_delay = 2.5
        
        # Define your three endpoints and API keys
        endpoints = [
            # Primary server
            {
                "url": "https://ai-riderhailingappai1824849404910565.openai.azure.com/openai/deployments/gpt-4o-rider-beta/chat/completions?api-version=2024-08-01-preview",
                "key": "44be3ea1f359415fb95bbe350580e1c2",
                "name": "PRIMARY"
            },
            # Second server
            {
                "url": "https://rider-m7ub39yn-japaneast.openai.azure.com/openai/deployments/gpt-4o-HH/chat/completions?api-version=2025-01-01-preview",
                "key": "ApPbfFKAWFVHasYuY9hXZSLL5cDLfrv8xu6VLrQgkveWWFCJue0sJQQJ99BCACi0881XJ3w3AAAAACOGyTEV",
                "name": "SECONDARY"
            },
            # Third server
            {
                "url": "https://ai-rider77895836543834.openai.azure.com/openai/deployments/gpt-4o-HHH/chat/completions?api-version=2025-01-01-preview",
                "key": "9ltHnSpSTCxnyA7EcGN2PWFX7UE1zBLKcIfCq1RGuNyXOJsSDS8zJQQJ99BCACYeBjFXJ3w3AAAAACOGlhwY",
                "name": "TERTIARY"
            }
        ]
        
        # Track which server to use - start with server index 0
        server_index = 0
        servers_tried = set()
        
        for attempt in range(max_retries):
            try:
                # Clear the set of tried servers at the beginning of each attempt
                if attempt > 0:
                    servers_tried = set()
                
                # Try each server until one succeeds or we've tried them all
                while server_index not in servers_tried and len(servers_tried) < len(endpoints):
                    # Get current server details
                    current_server = endpoints[server_index]
                    current_endpoint = current_server["url"]
                    current_api_key = current_server["key"]
                    server_name = current_server["name"]
                    
                    # Mark this server as tried
                    servers_tried.add(server_index)
                    
                    # Create headers for this server
                    current_headers = {
                        "Content-Type": "application/json",
                        "api-key": current_api_key,
                    }
                    
                    print(f"Using {server_name} server for URL: {image_url} (Attempt {attempt+1}/{max_retries})")

                    # Prepare the API call with the same prompt structure as before
                    prompt = self.create_extraction_prompt(image_url, "image", self.brand_details)
                    messages = [
                        {
                            "role": "system",
                            "content": "You are an expert creative analyst specialized in extracting structured information from advertising creatives."
                        },
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "text",
                                    "text": prompt
                                },
                                {
                                    "type": "image_url",
                                    "image_url": {
                                        "url": image_url
                                    }
                                }
                            ]
                        }
                    ]

                    # Make the API call
                    try:
                        async with self.http_session.post(
                            current_endpoint,
                            headers=current_headers,
                            json={
                                "messages": messages,
                                "temperature": 0.7,
                                "max_tokens": 4000,
                                "response_format": { "type": "json_object" }
                            },
                            timeout=aiohttp.ClientTimeout(total=300)
                        ) as response:
                            # Handle rate limiting
                            if response.status == 429:
                                print(f"Rate limited (429) on {server_name} server for URL: {image_url}")
                                server_index = (server_index + 1) % len(endpoints)
                                print(f"Switching to {endpoints[server_index]['name']} server")
                                continue
                            
                            # Handle other error status codes
                            if response.status != 200:
                                print(f"Error: API returned status code {response.status} on {server_name} server for URL: {image_url}")
                                server_index = (server_index + 1) % len(endpoints)
                                print(f"Server error, switching to {endpoints[server_index]['name']} server")
                                continue
                            
                            # Process successful response
                            result = await response.json()
                            
                            if 'choices' not in result or not result['choices']:
                                print(f"Error: API response missing expected content for URL: {image_url}")
                                server_index = (server_index + 1) % len(endpoints)
                                continue
                            
                            content_text = result['choices'][0]['message']['content']
                            
                            if not content_text or content_text.isspace():
                                print(f"Error: Empty content received from API for URL: {image_url}")
                                server_index = (server_index + 1) % len(endpoints)
                                continue
                            
                            try:
                                content = json.loads(content_text)
                                print(f"Successfully extracted content for URL: {image_url} using {server_name} server")
                            except json.JSONDecodeError as json_err:
                                print(f"JSON parsing error for URL {image_url}: {str(json_err)}")
                                print(f"Raw content: {content_text}")
                                
                                # Try to clean the content string and parse again
                                clean_content = content_text.strip()
                                if clean_content.startswith('```json'):
                                    clean_content = clean_content.split('```json', 1)[1]
                                if clean_content.endswith('```'):
                                    clean_content = clean_content.rsplit('```', 1)[0]
                                
                                clean_content = clean_content.strip()
                                print(f"Cleaned content: {clean_content[:500]}...")
                                
                                try:
                                    content = json.loads(clean_content)
                                    print(f"Successfully parsed JSON after cleaning for URL: {image_url}")
                                except json.JSONDecodeError:
                                    print(f"Failed to parse JSON even after cleaning for URL: {image_url}")
                                    server_index = (server_index + 1) % len(endpoints)
                                    continue
                            
                            # Return the parsed response with the same structure as before
                            return content
                    
                    except asyncio.TimeoutError:
                        print(f"Timeout error on {server_name} server for URL: {image_url}")
                        server_index = (server_index + 1) % len(endpoints)
                        continue
                        
                    except Exception as e:
                        print(f"Error with {server_name} server for URL {image_url}: {str(e)}")
                        traceback.print_exc()
                        server_index = (server_index + 1) % len(endpoints)
                        continue
                
                # If we've tried all servers in this attempt and all failed
                if len(servers_tried) == len(endpoints):
                    print(f"All servers failed for attempt {attempt+1}/{max_retries}")
                    
                    if attempt < max_retries - 1:
                        retry_delay = base_delay * (2 ** attempt)
                        print(f"Backing off for {retry_delay} seconds before next attempt...")
                        await asyncio.sleep(retry_delay)
                        server_index = (server_index + 1) % len(endpoints)
                    else:
                        print(f"All attempts with all servers failed for URL: {image_url}")
                        return None
                
            except Exception as e:
                print(f"Unexpected error in image analysis: {str(e)}")
                traceback.print_exc()
                
                if attempt < max_retries - 1:
                    retry_delay = base_delay * (2 ** attempt)
                    print(f"Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                    server_index = (server_index + 1) % len(endpoints)
                else:
                    return None
        
        print(f"All extraction attempts failed for URL: {image_url}")
        return None

    async def check_hash_exists(self, hash_value, page_id=None):
        """Check if hash exists and return extracted entities if found"""
        try:
            query = {"hash": hash_value}
            
            # If page_id is provided, try to find a match for both hash and page_id first
            if page_id:
                page_id_str = str(page_id)
                specific_match = await self.db.processed_creatives.find_one({"hash": hash_value, "page_id": page_id_str})
                if specific_match:
                    print(f"Found existing analysis for hash: {hash_value} and page_id: {page_id_str}")
                    return specific_match.get("extracted_entities")
            
            # Fall back to hash-only search if no page-specific match was found or no page_id was provided
            result = await self.db.processed_creatives.find_one(query)
            if result:
                print(f"Found existing analysis for hash: {hash_value}")
                return result.get("extracted_entities")
            return None
        except Exception as e:
            print(f"Error checking hash: {e}")
            return None

    async def save_creative_analysis(self, hash_value, entities, page_id=None, url=None):
        """Save hash and extracted entities separately"""
        try:
            doc = {
                "hash": hash_value,
                "extracted_entities": entities,
                "created_at": datetime.now()
            }
            
            # Add page_id if provided
            if page_id:
                doc["page_id"] = str(page_id)
                
            # Add creative url if provided
            if url:
                doc["creative_url"] = url
                
            await self.db.processed_creatives.update_one(
                {"hash": hash_value},
                {"$set": doc},
                upsert=True
            )
        except Exception as e:
            print(f"Error saving creative analysis: {e}")

    async def process_media(self, url, media_type, ad_id, ad_doc, hash_value=None):
        async with self.processing_lock:
            if url in self.currently_processing:
                print(f"Skipping {url} as it is already being processed")
                return None, None
            self.currently_processing.add(url)

        try:
            # Only update when starting to process this creative
            await self.update_analytics_status(
                page_id=ad_doc.get('page_id'),
                stage="analysis",
                status="processing",
                creative_url=url,
                media_type=media_type  # Pass the media type
            )
            
            # Generate hash if not provided
            if not hash_value:
                if media_type == "video":
                    hash_value = await asyncio.to_thread(download_video_chunk, url)
                else:
                    hash_value = await asyncio.to_thread(download_image_for_hash, url)

            # Check if we already have analysis for this hash
            existing_entities = await self.check_hash_exists(hash_value, ad_doc.get('page_id'))
            if existing_entities:
                print(f"Using existing analysis for {url} (hash: {hash_value})")
                # Don't update status for reused entries - we're not doing actual analysis
                
                extraction_result = {
                    "url": url,
                    "media_type": media_type,
                    "extracted_entities": existing_entities,
                    "ad_id": ad_id,
                    "page_id": str(ad_doc.get('page_id')),
                    "status": ad_doc.get('status'),
                    "platforms": ad_doc.get('platforms'),
                    "advertiser": ad_doc.get('advertiser'),
                    "description": ad_doc.get('description'),
                    "variation_count": ad_doc.get('variation_count'),
                    "cta_button": ad_doc.get('cta_button'),
                    "hash": hash_value,
                    "reused_analysis": True,
                    "start_date": ad_doc.get('start_date'),
                    "end_date": ad_doc.get('end_date'),
                    "active_duration": ad_doc.get('active_duration'),
                    "ad_text": ad_doc.get('ad_text'),
                    "ad_link": ad_doc.get('ad_link'),
                    "ad_cta": ad_doc.get('ad_cta'),
                    "timestamp": ad_doc.get('timestamp')
                }
                
                # Update in Meta_ads if exists, otherwise insert
                media_id = await self.update_or_insert_mongodb('Meta_ads', extraction_result, ad_id)

                if self.es_enabled:
                    if media_type == "image":
                        self.es_image_batch.append(extraction_result)
                    else:
                        self.es_video_batch.append(extraction_result)

                await self.save_creative_analysis(hash_value, existing_entities, ad_doc.get('page_id'), url)

                # Update status to completed for tracking, even though we reused analysis
                await self.update_analytics_status(
                    page_id=ad_doc.get('page_id'),
                    stage="analysis",
                    status="completed",
                    creative_url=url,
                    media_type=media_type
                )

                return extraction_result, media_id

            # If no existing analysis, perform new analysis
            extraction_result = {"url": url, "media_type": media_type}
            if media_type == "video":
                entities = await self.gemini_video_analysis(url)
            else:
                entities = await self.gemini_image_analysis(url)

            if not entities:
                print(f"Failed to extract entities for {url}")
                # Update status to failed for this creative
                await self.update_analytics_status(
                    page_id=ad_doc.get('page_id'),
                    stage="analysis",
                    status="failed",
                    creative_url=url,
                    media_type=media_type,
                    error="Failed to extract entities"
                )
                return None, None

            # Update status to completed for this specific creative
            await self.update_analytics_status(
                page_id=ad_doc.get('page_id'),
                stage="analysis",
                status="completed",
                creative_url=url,
                media_type=media_type
            )

            extraction_result.update({
                "extracted_entities": entities,
                "ad_id": ad_id,
                "page_id": str(ad_doc.get('page_id')),
                "status": ad_doc.get('status'),
                "platforms": ad_doc.get('platforms'),
                "advertiser": ad_doc.get('advertiser'),
                "description": ad_doc.get('description'),
                "variation_count": ad_doc.get('variation_count'),
                "cta_button": ad_doc.get('cta_button'),
                "hash": hash_value,
                "created_at": datetime.now(),
                "start_date": ad_doc.get('start_date'),
                "end_date": ad_doc.get('end_date'),
                "active_duration": ad_doc.get('active_duration'),
                "ad_text": ad_doc.get('ad_text'),
                "ad_link": ad_doc.get('ad_link'),
                "ad_cta": ad_doc.get('ad_cta'),
                "timestamp": ad_doc.get('timestamp')
            })

            # Update in Meta_ads if exists, otherwise insert
            media_id = await self.update_or_insert_mongodb('Meta_ads', extraction_result, ad_id)

            if self.es_enabled:
                if media_type == "image":
                    self.es_image_batch.append(extraction_result)
                else:
                    self.es_video_batch.append(extraction_result)

            await self.save_creative_analysis(hash_value, entities, ad_doc.get('page_id'), url)

            return extraction_result, media_id

        except Exception as e:
            print(f"Error processing media: {str(e)}")
            # Update status to error for this creative
            await self.update_analytics_status(
                page_id=ad_doc.get('page_id'),
                stage="analysis",
                status="error",
                creative_url=url,
                media_type=media_type,
                error=str(e)
            )
            return None, None
        finally:
            async with self.processing_lock:
                self.currently_processing.remove(url)

    async def update_or_insert_mongodb(self, collection_name, document, ad_id):
        """Update document if exists, otherwise insert"""
        try:
            client = AsyncIOMotorClient(MONGO_URI)
            db = client[database_name]
            collection = db[collection_name]
            
            # Try to update existing document
            result = await collection.update_one(
                {"ad_id": ad_id},
                {"$set": document},
                upsert=True
            )
            
            if result.modified_count > 0:
                print(f"Updated existing document for ad_id: {ad_id}")
            elif result.upserted_id:
                print(f"Inserted new document for ad_id: {ad_id}")
            
            return result.upserted_id or ad_id
            
        except Exception as e:
            print(f"Error in update_or_insert_mongodb: {e}")
            return None
        finally:
            client.close()

    async def process_ad(self, ad, page_id):
        if not ad:
            return None

        # Create the base ad document with all original fields
        ad_doc = {
            'library_id': ad.get('library_id'),
            'status': ad.get('status'),
            'start_date': ad.get('start_date'),
            'end_date': ad.get('end_date'),
            'active_duration': ad.get('active_duration'),
            'platforms': ad.get('platforms', []),
            'advertiser': ad.get('advertiser'),
            'description': ad.get('description'),
            'ad_text': ad.get('ad_text'),
            'ad_link': ad.get('ad_link'),
            'ad_cta': ad.get('ad_cta'),
            'cta_button': ad.get('cta_button'),
            'variation_count': ad.get('variation_count'),
            'timestamp': ad.get('timestamp'),
            'media_details': [],
            'extracted_entities': [],
            'page_id': page_id
        }

        media_details = ad.get('media_details', [])
        processed_media_details = []
        total_items = 0
        processed_items = 0

        # Count total items to process for progress tracking
        for media in media_details:
            if media.get('type') == 'carousel':
                total_items += len(media.get('items', []))
            else:
                total_items += 1
                
        print(f"Processing ad {ad.get('library_id')} with {total_items} media items")

        for media in media_details:
            media_type = media.get('type')
            
            if media_type == 'carousel':
                # Process each item in the carousel
                carousel_processed_items = []
                carousel_entities = []
                carousel_items = media.get('items', [])
                
                # Update status for carousel processing start
                await self.update_analytics_status(
                    page_id=page_id,
                    stage="analysis",
                    status="processing_carousel",
                    creative_url=f"Carousel with {len(carousel_items)} items",
                    media_type="carousel"
                )
                
                for item in carousel_items:
                    if 'image_src' in item:
                        # Update status with the carousel item type
                        await self.update_analytics_status(
                            page_id=page_id,
                            stage="analysis",
                            status="processing",
                            creative_url=item['image_src'],
                            media_type="carousel_item"
                        )
                        
                        result, _ = await self.process_media(
                            item['image_src'],
                            'image',
                            ad.get('library_id'),
                            ad_doc
                        )
                        if result:
                            # For carousel items, only store creative_url and extracted_entities
                            carousel_processed_items.append({
                                'creative_url': item['image_src'],
                                'extracted_entities': result.get('extracted_entities')
                            })
                            carousel_entities.append(result.get('extracted_entities'))
                            processed_items += 1
                        
                # Update status for carousel processing complete
                await self.update_analytics_status(
                    page_id=page_id,
                    stage="analysis",
                    status="carousel_completed",
                    creative_url=f"Carousel with {len(carousel_processed_items)} processed items",
                    media_type="carousel"
                )
                
                # Create processed carousel media with simplified structure and optional fields at carousel level
                processed_carousel = {
                    'type': 'carousel',
                    'items': carousel_processed_items,
                    'status': ad.get('status'),
                    'start_date': ad.get('start_date'),
                    'end_date': ad.get('end_date'),
                    'active_duration': ad.get('active_duration'),
                    'platforms': ad.get('platforms', []),
                    'advertiser': ad.get('advertiser'),
                    'description': ad.get('description'),
                    'ad_text': ad.get('ad_text'),
                    'ad_link': ad.get('ad_link'),
                    'ad_cta': ad.get('ad_cta'),
                    'cta_button': ad.get('cta_button'),
                    'variation_count': ad.get('variation_count'),
                    'timestamp': ad.get('timestamp'),
                    'page_id': page_id
                }
                processed_media_details.append(processed_carousel)
                
                # Add carousel analysis to extracted_entities
                ad_doc['extracted_entities'].append({
                    'type': 'carousel',
                    'items': carousel_entities
                })
                
            elif media_type == 'video':
                # Process video media
                if 'url' in media:
                    result, _ = await self.process_media(
                        media['url'],
                        'video',
                        ad.get('library_id'),
                        ad_doc
                    )
                    if result:
                        # Keep original video structure with hash and extracted entities, plus optional fields
                        media['hash'] = result.get('hash')
                        media['extracted_entities'] = result.get('extracted_entities')
                        media['status'] = ad.get('status')
                        media['start_date'] = ad.get('start_date')
                        media['end_date'] = ad.get('end_date')
                        media['active_duration'] = ad.get('active_duration')
                        media['platforms'] = ad.get('platforms', [])
                        media['advertiser'] = ad.get('advertiser')
                        media['description'] = ad.get('description')
                        media['ad_text'] = ad.get('ad_text')
                        media['ad_link'] = ad.get('ad_link')
                        media['ad_cta'] = ad.get('ad_cta')
                        media['cta_button'] = ad.get('cta_button')
                        media['variation_count'] = ad.get('variation_count')
                        media['timestamp'] = ad.get('timestamp')
                        media['page_id'] = page_id
                        processed_media_details.append(media)
                        processed_items += 1
                        
                        # Add video analysis to extracted_entities
                        ad_doc['extracted_entities'].append({
                            'type': 'video',
                            'url': media['url'],
                            'entities': result.get('extracted_entities')
                        })
                        
            elif media_type == 'image':
                # Process single image media
                if 'image_src' in media:
                    result, _ = await self.process_media(
                        media['image_src'],
                        'image',
                        ad.get('library_id'),
                        ad_doc
                    )
                    if result:
                        # Keep original image structure with hash and extracted entities, plus optional fields
                        media['hash'] = result.get('hash')
                        media['extracted_entities'] = result.get('extracted_entities')
                        media['status'] = ad.get('status')
                        media['start_date'] = ad.get('start_date')
                        media['end_date'] = ad.get('end_date')
                        media['active_duration'] = ad.get('active_duration')
                        media['platforms'] = ad.get('platforms', [])
                        media['advertiser'] = ad.get('advertiser')
                        media['description'] = ad.get('description')
                        media['ad_text'] = ad.get('ad_text')
                        media['ad_link'] = ad.get('ad_link')
                        media['ad_cta'] = ad.get('ad_cta')
                        media['cta_button'] = ad.get('cta_button')
                        media['variation_count'] = ad.get('variation_count')
                        media['timestamp'] = ad.get('timestamp')
                        media['page_id'] = page_id
                        processed_media_details.append(media)
                        processed_items += 1
                        
                        # Add image analysis to extracted_entities
                        ad_doc['extracted_entities'].append({
                            'type': 'image',
                            'url': media['image_src'],
                            'entities': result.get('extracted_entities')
                        })

        # Update the ad document with processed media details
        ad_doc['media_details'] = processed_media_details
        
        # Log completion info
        print(f"Completed processing ad {ad.get('library_id')}: {processed_items}/{total_items} items processed")
        
        # Update status for ad completion
        await self.update_analytics_status(
            page_id=page_id,
            stage="analysis",
            status="ad_completed",
            creative_url=f"Ad {ad.get('library_id')} ({processed_items}/{total_items} items)",
            media_type="ad"
        )
            
        return ad_doc

    async def process_ads_data(self, ads_data, page_id):
        ads_tasks = [self.process_ad(ad, page_id) for ad in ads_data]
        processed_ads = await asyncio.gather(*ads_tasks, return_exceptions=True)
        return [ad for ad in processed_ads if ad and not isinstance(ad, Exception)]

    async def bulk_insert_elasticsearch(self, documents, index_name, max_retries=5):
        # Check if there are documents to process and if the ES client is available
        if not documents or not self.es_client:
            print(f"No documents to index or Elasticsearch client not available for index: {index_name}")
            return
        
        try:
            # First verify the index exists
            index_exists = await self.es_client.indices.exists(index=index_name)
            if not index_exists:
                print(f"Index {index_name} does not exist. Creating it...")
                # Create the index with appropriate mapping
                if index_name == ELASTICSEARCH_IMAGE_INDEX:
                    await self.ensure_es_indexes()
                else:
                    print(f"Unknown index type: {index_name}")
                    return

            operations = []
            for doc in documents:
                # Ensure each document has a unique ID
                doc_id = doc.get("ad_id") or str(ObjectId())
                
                # Log the document being prepared for indexing
                print(f"Preparing document for index {index_name} with ID {doc_id}")
                
                # Append the index operation and document
                operations.append({"index": {"_index": index_name, "_id": doc_id}})
                operations.append(doc)
            
            # Proceed only if there are operations to perform
            if operations:
                print(f"Sending {len(documents)} documents to Elasticsearch index: {index_name}")
                
                # Perform the bulk insert with a timeout and refresh
                response = await asyncio.wait_for(
                    self.es_client.bulk(
                        operations=operations,
                        refresh=True,
                        request_timeout=1000
                    ),
                    timeout=1000
                )
                
                # Analyze the response
                successful = sum(1 for item in response["items"] if "error" not in item["index"])
                failed = len(response["items"]) - successful
                
                print(f"ES Bulk Insert Results for {index_name}:")
                print(f"- Total documents: {len(documents)}")
                print(f"- Successfully indexed: {successful}")
                print(f"- Failed to index: {failed}")
                
                # If there are failures, log the specific errors
                if failed > 0:
                    print(f"\nFailed documents in {index_name}:")
                    for item in response["items"]:
                        if "error" in item["index"]:
                            print(f"Document ID: {item['index']['_id']}")
                            print(f"Error: {item['index']['error']}")
                            print("---")
                
                # Verify that documents were actually indexed
                if successful == 0 and failed == 0:
                    print(f"Warning: No documents processed by Elasticsearch for index {index_name}")
                    print(f"Response: {response}")
                    
                    # Try to get index stats to verify
                    try:
                        stats = await self.es_client.indices.stats(index=index_name)
                        print(f"Index stats: {stats}")
                    except Exception as e:
                        print(f"Error getting index stats: {e}")
            
            else:
                print(f"No operations prepared for index {index_name}")
        
        except asyncio.TimeoutError:
            print(f"Timeout while bulk inserting to Elasticsearch index {index_name}")
        except Exception as e:
            print(f"Error in Elasticsearch bulk insert for index {index_name}: {str(e)}")
            traceback.print_exc()

    def convert_date_to_iso(self, date_str):
        """Convert date string to ISO format for Elasticsearch"""
        if not date_str:
            return None
        try:
            # Try parsing various date formats
            if isinstance(date_str, datetime):
                return date_str.isoformat()
            
            # Try DD MMM YYYY format
            try:
                date_obj = datetime.strptime(date_str, '%d %b %Y')
                return date_obj.isoformat()
            except ValueError:
                # Try YYYY-MM-DD format
                try:
                    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                    return date_obj.isoformat()
                except ValueError:
                    # Try DD/MM/YYYY format
                    try:
                        date_obj = datetime.strptime(date_str, '%d/%m/%Y')
                        return date_obj.isoformat()
                    except ValueError:
                        print(f"Warning: Could not parse date string: {date_str}")
                        return None
        except Exception as e:
            print(f"Warning: Error parsing date {date_str}: {str(e)}")
            return None

    async def flush_batches(self):
        if self.es_enabled and self.es_image_batch:
            es_docs = []
            for item in self.es_image_batch:
                # Extract the document directly without doc wrapper
                doc = item if isinstance(item, dict) else item.get("doc", {})
                entities = doc.get("extracted_entities", {})
                
                # Convert dates to ISO format
                start_date = self.convert_date_to_iso(doc.get("start_date"))
                end_date = self.convert_date_to_iso(doc.get("end_date"))
                active_duration = doc.get("active_duration")
                
                es_doc = {
                    "url": doc.get("url"),
                    "ad_id": doc.get("ad_id"),
                    "media_type": doc.get("media_type"),
                    "time": doc.get("time"),
                    "hash": doc.get("hash"),
                    "page_id": str(doc.get("page_id")),  # Ensure page_id is stored as string
                    "status": doc.get("status"),
                    "cta_button": doc.get("cta_button", ""),
                    "start_date": start_date,
                    "end_date": end_date,
                    "active_duration": active_duration,
                    "H1": entities.get("H1", ""),
                    "H1_styles": entities.get("H1_styles", {
                        "font_style": "",
                        "color": "",
                        "formatting": [],
                        "position": ""
                    }),
                    "H2": entities.get("H2", ""),
                    "H3": entities.get("H3", ""),
                    "Objective": entities.get("Objective", ""),
                    "Subjects": entities.get("Subjects", []),
                    "Colour_schema": entities.get("Colour_schema", []),
                    "Emotional_appeal": entities.get("Emotional_appeal", []),
                    "brand_colors": entities.get("brand_colors", []),
                    "usps": entities.get("usps", ""),
                    "hook": entities.get("hook", ""),
                    "title": entities.get("title", ""),
                    "theme": entities.get("theme", ""),
                    "keywords": entities.get("keywords", []),
                    "tone_and_style": entities.get("tone_and_style", {
                        "tone": "",
                        "writing_style": "",
                        "visual_style": ""
                    }),
                    "visual_focus": entities.get("visual_focus", {
                        "main_element": "",
                        "focus_area": "",
                        "visual_hierarchy": ""
                    }),
                    "product_categories": entities.get("product_categories", []),
                    "product_features": entities.get("product_features", []),
                    "conversion_elements": entities.get("conversion_elements", {}),
                    "audience_targeting": entities.get("audience_targeting", {}),
                    "ad_text": doc.get("ad_text", ""),
                    "ad_link": doc.get("ad_link", ""),
                    "ad_cta": doc.get("ad_cta", ""),
                    "timestamp": doc.get("timestamp"),
                    "platforms": doc.get("platforms", []),
                    "advertiser": doc.get("advertiser", ""),
                    "ad_description": doc.get("description", ""),  # Renamed from description to ad_description
                    "variation_count": doc.get("variation_count", 0)
                }
                es_docs.append(es_doc)
            if es_docs:
                await self.bulk_insert_elasticsearch(es_docs, ELASTICSEARCH_IMAGE_INDEX)
            self.es_image_batch = []

        if self.es_enabled and self.es_video_batch:
            es_docs = []
            for item in self.es_video_batch:
                # Extract the document directly without doc wrapper
                doc = item if isinstance(item, dict) else item.get("doc", {})
                entities = doc.get("extracted_entities", {})
                
                # Convert dates to ISO format
                start_date = self.convert_date_to_iso(doc.get("start_date"))
                end_date = self.convert_date_to_iso(doc.get("end_date"))
                active_duration = doc.get("active_duration")
                
                es_doc = {
                    "url": doc.get("url"),
                    "ad_id": doc.get("ad_id"),
                    "media_type": doc.get("media_type"),
                    "time": doc.get("time"),
                    "hash": doc.get("hash"),
                    "page_id": str(doc.get("page_id")),  # Ensure page_id is stored as string
                    "status": doc.get("status"),
                    "cta_button": doc.get("cta_button", ""),
                    "start_date": start_date,
                    "end_date": end_date,
                    "active_duration": active_duration,
                    "hook": entities.get("hook", ""),
                    "advertisement_type": entities.get("advertisement_type", ""),
                    "motto": entities.get("motto", ""),
                    "title": entities.get("title", ""),
                    "keywords": entities.get("keywords", []),
                    "theme": entities.get("theme", ""),
                    "summary": entities.get("summary", ""),
                    "target_audience": entities.get("target_audience", ""),
                    "call_to_action": entities.get("call_to_action", ""),
                    "tone_and_style": entities.get("tone_and_style", {
                        "tone": "",
                        "writing_style": "",
                        "visual_style": ""
                    }),
                    "visual_focus": entities.get("visual_focus", {
                        "main_element": "",
                        "focus_area": "",
                        "visual_hierarchy": ""
                    }),
                    "h1_styles": entities.get("h1_styles", {
                        "font_style": "",
                        "color": "",
                        "formatting": [],
                        "position": ""
                    }),
                    "unique_selling_proposition": entities.get("unique_selling_proposition", ""),
                    "main_topic": entities.get("main_topic", ""),
                    "objective": entities.get("objective", ""),
                    "description": {
                        "subjects": entities.get("description", {}).get("subjects", []),
                        "colour_schema": entities.get("description", {}).get("colour_schema", []),
                        "emotional_appeal": entities.get("description", {}).get("emotional_appeal", [])
                    },
                    "narrative_analysis": entities.get("narrative_analysis", {}),
                    "user_journey": entities.get("user_journey", {}),
                    "conversion_path": entities.get("conversion_path", {}),
                    "key_moments": entities.get("key_moments", []),
                    "product_categories": entities.get("product_categories", []),
                    "product_features": entities.get("product_features", []),
                    "ad_text": doc.get("ad_text", ""),
                    "ad_link": doc.get("ad_link", ""),
                    "ad_cta": doc.get("ad_cta", ""),
                    "timestamp": doc.get("timestamp"),
                    "platforms": doc.get("platforms", []),
                    "advertiser": doc.get("advertiser", ""),
                    "ad_description": doc.get("description", ""),  # Renamed from description to ad_description
                    "variation_count": doc.get("variation_count", 0)
                }
                es_docs.append(es_doc)
            if es_docs:
                await self.bulk_insert_elasticsearch(es_docs, ELASTICSEARCH_VIDEO_INDEX)
            self.es_video_batch = []

        if self.mongodb_batch:
            batch_to_insert = self.mongodb_batch.copy()
            self.mongodb_batch = []
            await self.batch_insert_mongodb('competitors_creatives', batch_to_insert)

        print(f"✅ Flushed batches: Image ES: {len(self.es_image_batch)}, Video ES: {len(self.es_video_batch)}, MongoDB: {len(batch_to_insert) if 'batch_to_insert' in locals() else 0}")

    async def batch_insert_mongodb(self, collection_name, documents, max_retries=5, wait_time=1):
        if not documents:
            return []
        attempt = 0
        while attempt < max_retries:
            try:
                client = AsyncIOMotorClient(MONGO_URI)
                db = client[database_name]
                collection = db[collection_name]
                for doc in documents:
                    if "_id" not in doc:
                        doc["_id"] = ObjectId()
                result = await collection.insert_many(documents)
                print(f"MongoDB: Inserted {len(result.inserted_ids)} documents")
                return result.inserted_ids
            except Exception as e:
                attempt += 1
                print(f"Attempt {attempt} failed: {str(e)}")
                if attempt < max_retries:
                    wait = min(wait_time * (2 ** (attempt - 1)), 10)
                    print(f"Retrying in {wait} seconds...")
                    await asyncio.sleep(wait)
                else:
                    print("Max retries reached. Insertion failed.")
                    return []
            finally:
                client.close()

    async def docreativeanalysis(self, ads_data, page_id):
        try:
            print("\n=== Starting Creative Analysis ===")
            print(f"Initializing analysis for {len(ads_data)} ads")
            
            # Update analytics status to started
            await self.update_analytics_status(
                page_id=page_id,
                stage="analysis",
                status="started"
            )
            
            # Delete existing data first
            print("\n=== Deleting Existing Data ===")
            await self.delete_existing_data(page_id)
            
            await self.initialize()
            if not ads_data:
                print("No ads data found to process")
                # Update status to failed if no data
                await self.update_analytics_status(
                    page_id=page_id,
                    stage="analysis",
                    status="failed",
                    error="No ads data found"
                )
                return {"success": False, "error": "No ads data found"}

            print("\n=== Processing Ads ===")
            # Track progress
            total_ads = len(ads_data)
            current_ad = 0
            
            # Process ads one by one for better tracking
            processed_ads = []
            for ad in ads_data:
                current_ad += 1
                print(f"\n--- Processing Ad {current_ad}/{total_ads} (ID: {ad.get('library_id')}) ---")
                
                # Update progress status
                await self.update_analytics_status(
                    page_id=page_id,
                    stage="analysis",
                    status="processing_ad",
                    creative_url=f"Ad {current_ad}/{total_ads} (ID: {ad.get('library_id')})",
                    media_type="ad_batch"
                )
                
                # Process this ad
                processed_ad = await self.process_ad(ad, page_id)
                if processed_ad:
                    processed_ads.append(processed_ad)
                
                # Periodically flush batches to avoid memory issues
                if current_ad % 5 == 0 or current_ad == total_ads:
                    await self.flush_batches()
            
            # Save raw data for reference
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            print(f"\n=== Saving Raw Data ===")
            print(f"Saving raw data with timestamp: {timestamp}")
            await self.save_json_file('raw_data', ads_data, timestamp)
            
            # Calculate success metrics
            success_count = len([ad for ad in processed_ads if ad])
            failed_count = total_ads - success_count
            success_rate = (success_count / total_ads * 100) if total_ads > 0 else 0
            
            print("\n=== Analysis Summary ===")
            print(f"Total ads processed: {total_ads}")
            print(f"Successfully processed: {success_count} ({success_rate:.1f}%)")
            print(f"Failed to process: {failed_count}")

            # Update analytics status to completed with summary
            await self.update_analytics_status(
                page_id=page_id,
                stage="analysis",
                status="completed"
            )
            
            # Add detailed analysis completion data
            await self.update_analysis_completion_data(
                page_id=page_id,
                total_ads=total_ads,
                success_count=success_count,
                failed_count=failed_count,
                success_rate=success_rate,
                timestamp=timestamp
            )

            return {
                "success": True,
                "processed_ads": processed_ads,
                "total_ads": total_ads,
                "successfully_processed": success_count,
                "failed_to_process": failed_count,
                "success_rate": f"{success_rate:.1f}%"
            }
        except Exception as e:
            print(f"\n❌ Error in docreativeanalysis: {str(e)}")
            traceback.print_exc()
            
            # Update status to error
            await self.update_analytics_status(
                page_id=page_id,
                stage="analysis",
                status="error",
                error=str(e)
            )
            
            return {"success": False, "error": f"Analysis failed: {str(e)}"}
        finally:
            try:
                print("\n=== Flushing Final Batches ===")
                await self.flush_batches()
            except Exception as flush_error:
                print(f"❌ Error flushing final batches: {flush_error}")
            await self.cleanup()
            
    async def update_analysis_completion_data(self, page_id, total_ads, success_count, failed_count, success_rate, timestamp):
        """Update detailed completion data in MongoDB"""
        try:
            # Ensure page_id is a string
            page_id = str(page_id)
            collection = self.test_db['metaadslibraries']
            
            # Update document with detailed completion data
            update_data = {
                "$set": {
                    "analytics_data.completion_details": {
                        "total_ads": total_ads,
                        "success_count": success_count,
                        "failed_count": failed_count, 
                        "success_rate": f"{success_rate:.1f}%",
                        "completion_timestamp": datetime.now(),
                        "raw_data_timestamp": timestamp
                    },
                    "analytics_data.analysis_complete": True
                }
            }
            
            await collection.update_one(
                {"page_id": page_id},
                update_data,
                upsert=True
            )
            
            print(f"✅ Updated detailed completion data for page {page_id}")
            
        except Exception as e:
            print(f"❌ Error updating completion data: {str(e)}")
            traceback.print_exc()

    async def save_json_file(self, file_type, data, timestamp):
        def object_id_converter(obj):
            if isinstance(obj, ObjectId):
                return str(obj)
            elif isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Type {type(obj)} not serializable")

        filename = f'{file_type}_{timestamp}.json'
        async with aiofiles.open(filename, 'w') as f:
            await f.write(json.dumps(data, indent=4, default=object_id_converter))

    async def check_existing_brand_analysis(self):
        """Check if brand analysis exists and is recent (within 7 days)"""
        try:
            # Check both by brand_id and URL
            existing_analysis = await self.db.brand_analysis.find_one({
                '$or': [
                    {'_id': self.brand_id},
                    {'brand_url': self.brand_url}
                ]
            })
            
            if existing_analysis:
                # Check if analysis is older than 7 days
                last_updated = existing_analysis.get('updated_at')
                if last_updated:
                    time_difference = datetime.now() - last_updated
                    if time_difference.days >= 7:
                        print(f"⚠️ Brand analysis is {time_difference.days} days old. Need to reanalyze.")
                        return None
                    else:
                        print(f"✅ Found recent brand analysis ({time_difference.days} days old) for URL: {self.brand_url}")
                        # Update brand_id if found by URL but different brand_id
                        if existing_analysis.get('_id') != self.brand_id:
                            await self.db.brand_analysis.update_one(
                                {'_id': existing_analysis['_id']},
                                {'$addToSet': {'associated_brand_ids': self.brand_id}}
                            )
                        return existing_analysis
                else:
                    print("⚠️ No timestamp found in existing analysis. Need to reanalyze.")
                    return None
            return None
        except Exception as e:
            print(f"❌ Error checking existing brand analysis: {str(e)}")
            return None

    async def analyze_brand(self):
        """Analyze brand before processing ads"""
        try:
            # First check if we have recent analysis (within 7 days)
            existing_analysis = await self.check_existing_brand_analysis()
            
            if existing_analysis:
                # Use existing recent analysis
                self.brand_details = {
                    "brand_id": self.brand_id,
                    "core_identity": existing_analysis.get('coreIdentity', {}),
                    "brand_as_product": existing_analysis.get('brandAsProduct', {}),
                    "brand_as_person": existing_analysis.get('brandAsPerson', {}),
                    "brand_as_symbol": existing_analysis.get('brandAsSymbol', {}),
                    "product_categories": existing_analysis.get('products', [])
                }
                print(f"✅ Using recent brand analysis for: {self.brand_url}")
                print(f"✅ Product categories loaded: {self.brand_details['product_categories']}")
                return

            # If no existing analysis or it's too old, perform new analysis
            # Extract brand name from URL properly
            try:
                # Remove http:// or https:// and get domain
                domain = self.brand_url.split('//')[1].split('.')[0]
                # Skip 'www' if it's the first part of the domain
                if domain.lower() == 'www':
                    # Get the actual domain name after www
                    domain = self.brand_url.split('//')[1].split('.')[1]
                # Convert domain to title case and replace hyphens/underscores with spaces
                brand_name = domain.replace('-', ' ').replace('_', ' ').title()
                print(f"✅ Extracted brand name from URL: {brand_name}")
            except Exception as e:
                print(f"⚠️ Could not extract brand name from URL: {str(e)}")
                brand_name = "Unknown Brand"

            
            
            brand_analysis = self.brand_analyzer.about_brand(brand_name, self.brand_url)
            
            if brand_analysis.get('status') == 'success':
                current_time = datetime.now()
                # Add additional metadata
                brand_analysis['_id'] = self.brand_id
                brand_analysis['brand_url'] = self.brand_url
                brand_analysis['associated_brand_ids'] = [self.brand_id]
                brand_analysis['created_at'] = current_time
                brand_analysis['updated_at'] = current_time
                
                # Store brand analysis in MongoDB
                await self.db.brand_analysis.update_one(
                    {'_id': self.brand_id},
                    {
                        '$set': brand_analysis,
                        '$setOnInsert': {'first_analyzed_at': current_time}
                    },
                    upsert=True
                )
                
                # Set brand details for creative analysis
                self.brand_details = {
                    "brand_id": self.brand_id,
                    "core_identity": brand_analysis.get('coreIdentity', {}),
                    "brand_as_product": brand_analysis.get('brandAsProduct', {}),
                    "brand_as_person": brand_analysis.get('brandAsPerson', {}),
                    "brand_as_symbol": brand_analysis.get('brandAsSymbol', {}),
                    "product_categories": brand_analysis.get('products', [])
                }
                print(f"✅ New brand analysis completed for: {brand_name}")
                print(f"✅ Product categories loaded: {self.brand_details['product_categories']}")
            else:
                print(f"⚠️ Brand analysis failed: {brand_analysis.get('message')}")
                # If new analysis fails, try to use any existing analysis even if outdated
                fallback_analysis = await self.db.brand_analysis.find_one({
                    '$or': [
                        {'_id': self.brand_id},
                        {'brand_url': self.brand_url}
                    ]
                })
                if fallback_analysis:
                    self.brand_details = {
                        "brand_id": self.brand_id,
                        "core_identity": fallback_analysis.get('coreIdentity', {}),
                        "brand_as_product": fallback_analysis.get('brandAsProduct', {}),
                        "brand_as_person": fallback_analysis.get('brandAsPerson', {}),
                        "brand_as_symbol": fallback_analysis.get('brandAsSymbol', {}),
                        "product_categories": fallback_analysis.get('products', [])
                    }
                    print(f"⚠️ Using existing brand analysis (possibly outdated) after failed update attempt")
                    print(f"Last updated: {fallback_analysis.get('updated_at')}")
            
        except Exception as e:
            print(f"❌ Error in brand analysis: {str(e)}")
            traceback.print_exc()

    async def delete_existing_data(self, page_id):
        """Delete existing data from Meta_ads and Elasticsearch for a given page_id"""
        try:
            # Delete from MongoDB Meta_ads collection
            mongo_client = AsyncIOMotorClient(MONGO_URI)
            db = mongo_client[database_name]
            meta_ads_collection = db['Meta_ads']
            
            # Delete all documents with matching page_id
            delete_result = await meta_ads_collection.delete_many({"page_id": str(page_id)})
            print(f"Deleted {delete_result.deleted_count} documents from Meta_ads collection")
            
            # Delete from Elasticsearch if enabled
            if self.es_enabled and self.es_client:
                try:
                    # Delete from image index
                    image_delete_result = await self.es_client.delete_by_query(
                        index=ELASTICSEARCH_IMAGE_INDEX,
                        body={
                            "query": {
                                "term": {
                                    "page_id": str(page_id)
                                }
                            }
                        },
                        refresh=True  # Force refresh to ensure deletion is complete
                    )
                    print(f"Deleted {image_delete_result['deleted']} documents from image index")
                    
                    # Delete from video index
                    video_delete_result = await self.es_client.delete_by_query(
                        index=ELASTICSEARCH_VIDEO_INDEX,
                        body={
                            "query": {
                                "term": {
                                    "page_id": str(page_id)
                                }
                            }
                        },
                        refresh=True  # Force refresh to ensure deletion is complete
                    )
                    print(f"Deleted {video_delete_result['deleted']} documents from video index")
                    
                except Exception as es_error:
                    print(f"❌ Error deleting from Elasticsearch: {str(es_error)}")
                    traceback.print_exc()
                
            print(f"✅ Successfully deleted existing data for page_id: {page_id}")
            
        except Exception as e:
            print(f"❌ Error deleting existing data: {str(e)}")
            traceback.print_exc()
        finally:
            if 'mongo_client' in locals():
                mongo_client.close()

    async def update_analytics_status(self, page_id, stage, status, creative_url=None, error=None, media_type=None):
        """Update analytics status in MongoDB - simple version that only tracks current creative"""
        try:
            # Ensure we have a valid page_id
            if not page_id:
                print("⚠️ No page_id provided for status update")
                return
                
            # Ensure page_id is a string
            page_id = str(page_id)
            collection = self.test_db['metaadslibraries']
            
            # Base update data with common fields
            update_data = {
                "$set": {
                    "analytics_data.current_stage": stage,
                    "analytics_data.status": status,
                    "analytics_data.last_updated": datetime.now()
                }
            }
            
            # Add different fields based on status type
            if status == "processing" and creative_url:
                # Only update current creative when processing a specific creative
                update_data["$set"]["analytics_data.current_creative"] = creative_url
                update_data["$set"]["analytics_data.current_media_type"] = media_type or "unknown"
                print(f"⏳ Processing creative: {creative_url} (Type: {media_type})")
                
            elif status == "completed" and creative_url:
                # For completed status with a creative, update last completed creative
                update_data["$set"]["analytics_data.last_completed_creative"] = creative_url
                update_data["$set"]["analytics_data.last_completed_time"] = datetime.now()
                update_data["$inc"] = {"analytics_data.completed_count": 1}
                print(f"✅ Completed analysis of: {creative_url}")
                
            elif status == "failed" and creative_url:
                # For failed status, track failures
                update_data["$set"]["analytics_data.last_failed_creative"] = creative_url
                update_data["$set"]["analytics_data.last_error"] = error or "Unknown error"
                update_data["$inc"] = {"analytics_data.failed_count": 1}
                print(f"❌ Failed analysis of: {creative_url}")
                
            elif status == "error" and creative_url:
                # For error status, track errors
                update_data["$set"]["analytics_data.last_error_creative"] = creative_url
                update_data["$set"]["analytics_data.last_error"] = error or "Unknown error"
                update_data["$inc"] = {"analytics_data.error_count": 1}
                print(f"❌ Error during analysis of: {creative_url}: {error}")
                
            elif status == "started":
                # For overall process start
                update_data["$set"]["analytics_data.start_time"] = datetime.now()
                update_data["$set"]["analytics_data.completed_count"] = 0
                update_data["$set"]["analytics_data.failed_count"] = 0
                update_data["$set"]["analytics_data.error_count"] = 0
                print(f"🚀 Started analysis for page: {page_id}")
                
            elif status == "completed" and not creative_url:
                # For overall process completion
                update_data["$set"]["analytics_data.complete_time"] = datetime.now()
                update_data["$set"]["analytics_data.all_completed"] = True
                print(f"🏁 Completed all analysis for page: {page_id}")
                
            # Update the document
            await collection.update_one(
                {"page_id": page_id},
                update_data,
                upsert=True
            )
                
        except Exception as e:
            print(f"❌ Error updating analytics status: {str(e)}")
            traceback.print_exc()

@asynccontextmanager
async def analysis_session(brand_id, brand_url=None):
    analysis = CreativeAnalysis(brand_id, brand_url)
    try:
        await analysis.initialize()
        yield analysis
    finally:
        await analysis.cleanup()

async def run_async_analysis(brand_id, ads_data, brand_url, page_id):
    try:
        async with analysis_session(brand_id, brand_url) as analysis:
            test_data = await analysis.docreativeanalysis(ads_data, page_id)
            if not test_data:
                raise ValueError("No test data returned from analysis")
            return {
                "success": True,
                "data": test_data,
            }
    except Exception as e:
        print(f"Error during analysis: {str(e)}")
        traceback.print_exc()
        return {
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }

async def fetch_ads_data_from_mongodb(page_id):
    """Fetch ads data for a specific page from MongoDB"""
    try:
        client = AsyncIOMotorClient(MONGO_URI)
        db = client['extension_data']
        collection = db['metaads']
        
        query = {"page_id": page_id}
        doc = await collection.find_one(query)
        
        if not doc:
            print(f"No ads found for page ID: {page_id}")
            return None
            
        ads_data = doc.get('ads_data', [])
        total_ads = len(ads_data)
        print(f"\n=== Starting Ad Processing ===")
        print(f"Total ads found: {total_ads}")
        
        # Process all ads without date filtering
        print(f"\n=== Processing All Ads ===")
        print(f"Total ads to process: {total_ads}")
        
        return ads_data
        
    except Exception as e:
        print(f"Error fetching ads data: {e}")
        traceback.print_exc()
        return None
    finally:
        client.close()

# Define async function to encapsulate the analysis logic
async def run_analysis(brand_id, page_id, url):
    """
    Asynchronous function to perform creative analysis.
    
    Args:
        brand_id (str): The brand identifier.
        page_id (str): The page identifier.
        url (str): The URL to analyze.
    
    Returns:
        dict: Result of the analysis.
    
    Raises:
        ValueError: If ads data is not found or analysis fails.
    """
    try:
        # Fetch ads data from MongoDB
        ads_data = await fetch_ads_data_from_mongodb(page_id)
        if not ads_data:
            return {
                "success": False,
                "error": f"No ads data found for page ID: {page_id}"
            }
            
        # Run analysis
        result = await run_async_analysis(brand_id, ads_data, url, page_id)
        
        if not result or not result.get("success"):
            error_message = result.get('error', 'Analysis failed without specific error') if result else 'No result returned'
            return {
                "success": False,
                "error": error_message
            }
        else:
            print(f"Analysis completed successfully for page_id: {page_id}")
            print(f"Total ads: {result.get('data', {}).get('total_ads', 0)}")
            print(f"Processed ads: {result.get('data', {}).get('successfully_processed', 0)}")
            print(f"Failed ads: {result.get('data', {}).get('failed_to_process', 0)}")
            
            return result
    except Exception as e:
        print(f"Error in analysis: {str(e)}")
        traceback.print_exc()
        return {
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }

# Define the Celery task to perform the analysis
@celery_app.task(name='perform_analysis', bind=True, max_retries=3)
def perform_analysis(self, brand_id, page_id, url):
    """
    Celery task to execute the creative analysis in the background.
    
    Args:
        brand_id (str): The brand identifier.
        page_id (str): The page identifier.
        url (str): The URL to analyze.
    
    Returns:
        dict: Result of the analysis.
    """
    try:
        # Run the async analysis function within the Celery task
        result = asyncio.run(run_analysis(brand_id, page_id, url))
        return result
    except Exception as e:
        print(f"Error in Celery task: {str(e)}")
        traceback.print_exc()
        # Retry the task on failure with a 60-second delay
        raise self.retry(exc=e, countdown=60)

@app.route('/', methods=['GET'])
def index():
    return jsonify({"success": True, "message": "Creative Extraction API is running"}), 200

@app.route('/analyze', methods=['POST'])
def analyze():
    try:
        data = request.get_json()
        
        # Validate input parameters
        if not data or 'page_id' not in data:
            return jsonify({"success": False, "error": "Missing required parameter: page_id"}), 400
            
        page_id = data.get('page_id')
        url = data.get('url')
        
        if not url:
            return jsonify({"success": False, "error": "Missing required parameter: url"}), 400
            
        # Log the request
        print(f"\n=== New Analysis Request ===")
        print(f"Page ID: {page_id}")
        print(f"URL: {url}")
        
        # Use page_id as brand_id (consistent with original logic)
        brand_id = page_id
        
        # Queue the task with Celery
        task = perform_analysis.delay(brand_id, page_id, url)
        
        # Return response with task ID
        return jsonify({
            "success": True,
            "message": "Analysis queued",
            "page_id": page_id,
            "url": url,
            "task_id": task.id,
            "status": "processing"
        }), 202  # 202 Accepted status code for queued tasks
        
    except Exception as e:
        print(f"Error processing request: {str(e)}")
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

# Flask endpoint to check task status
@app.route('/status/<task_id>', methods=['GET'])
def check_status(task_id):
    """
    Endpoint to check the status of a Celery task by its ID.
    
    Args:
        task_id (str): The ID of the Celery task.
    
    Returns:
        JSON response with task status and result (if completed).
    """
    task = perform_analysis.AsyncResult(task_id)
    
    if task.state == 'PENDING':
        response = {"success": True, "status": "queued"}
    elif task.state == 'STARTED':
        response = {"success": True, "status": "processing"}
    elif task.state == 'SUCCESS':
        response = {"success": True, "status": "completed", "result": task.result}
    elif task.state == 'FAILURE':
        response = {"success": False, "status": "failed", "error": str(task.result)}
    else:
        response = {"success": False, "status": task.state}
    
    return jsonify(response), 200

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    debug = os.getenv("FLASK_DEBUG", "1") == "1"
    app.run(host='0.0.0.0', port=port, debug=debug)
    


