from openai import OpenAI
import re
from datetime import datetime
import json
import requests
from flask import jsonify
from typing import Dict, List

class BrandAnalyzer:
    def __init__(self):
        # Azure OpenAI Configuration
        self.endpoint = "https://ai-riderhailingappai1824849404910565.openai.azure.com/openai/deployments/gpt-4o-rider-beta/chat/completions?api-version=2024-08-01-preview"
        self.api_key = "44be3ea1f359415fb95bbe350580e1c2"
        self.headers = {
            "Content-Type": "application/json",
            "api-key": self.api_key,
        }
        
        # Perplexity Configuration
        self.perplexity_key = "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"
        self.perplexity_client = OpenAI(
            api_key=self.perplexity_key,
            base_url="https://api.perplexity.ai"
        )

    def get_brand_info_from_perplexity(self, brand_name: str) -> Dict:
        try:
            # First message to get brand information
            brand_info_messages = [
                {
                    "role": "system",
                    "content": (
                        "You are an expert business analyst. Provide accurate and structured information about "
                        "the brand that focuses on their core identity, brand as product, brand as person, "
                        "and brand as symbol aspects. Use only factual, verifiable information."
                    )
                },
                {
                    "role": "user",
                    "content": (
                        f"Research and provide detailed information about {brand_name} covering these specific aspects:\n\n"
                        "1. Core Identity:\n"
                        "- Main website URL\n"
                        "- Brand vision and mission statements\n"
                        "- Core values and principles\n\n"
                        "2. Brand as Product:\n"
                        "- Main product/service categories\n"
                        "- Market positioning (Premium/High-Quality/Mid-Range/Value)\n"
                        "- Key product features and attributes\n"
                        "- Primary customer benefits\n\n"
                        "3. Brand as Person:\n"
                        "- Brand personality traits\n"
                        "- Communication style and tone of voice\n"
                        "- Customer relationship approach\n\n"
                        "4. Brand as Symbol:\n"
                        "- Brand colors (if mentioned)\n"
                        "- Typography and visual style\n"
                        "- Logo and brand asset information\n\n"
                        "Please provide specific, factual information and avoid speculation. "
                        "Include direct quotes from official sources when available."
                    )
                }
            ]
            
            brand_info_response = self.perplexity_client.chat.completions.create(
                model="llama-3.1-sonar-large-128k-online",
                messages=brand_info_messages,
            )
            brand_info = brand_info_response.choices[0].message.content
            
            # Second message specifically for product categories with improved prompt
            product_messages = [
                {
                    "role": "system",
                    "content": (
                        "You are a product catalog expert. Your task is to list all product categories from the brand's catalog. "
                        "Rules:\n"
                        "1. List only actual product categories\n"
                        "2. Include both specific brand categories and common industry categories\n"
                        "3. Focus on main product lines and their variants\n"
                        "4. Include all product categories that can be purchased from the brand\n"
                        "5. Add common industry categories that are relevant to the brand's market"
                    )
                },
                {
                    "role": "user",
                    "content": (
                        f"List all product categories offered by {brand_name}.\n"
                        "Include both:\n"
                        "1. Brand-specific product categories\n"
                        "2. Common industry categories relevant to the brand\n\n"
                        "Format the response as a JSON array of category names.\n"
                        "Cover all product categories from the brand's website.\n"
                        "Make sure to include:\n"
                        "- Main product lines\n"
                        "- Product variants\n"
                        "- Common industry categories\n"
                    )
                }
            ]
            
            product_response = self.perplexity_client.chat.completions.create(
                model="llama-3.1-sonar-large-128k-online",
                messages=product_messages,
            )
            
            # Extract product categories from response with improved parsing
            product_content = product_response.choices[0].message.content
            try:
                # Try to parse if it's already in JSON format
                product_categories = json.loads(product_content)
            except json.JSONDecodeError:
                # If not JSON, use more robust extraction
                # First, try to find a list-like structure
                matches = re.findall(r'[\["\'](.*?)[\]"\']|[-•*] (.*?)(?=[-•*]|\n|$)', product_content)
                product_categories = []
                for match in matches:
                    # Process each match
                    for item in match:
                        if item:
                            # Clean up the item
                            cleaned_item = item.strip(' -"[]\'')
                            if cleaned_item and len(cleaned_item) > 2:  # Avoid very short strings
                                product_categories.append(cleaned_item)
                
                # Remove duplicates while preserving order
                product_categories = list(dict.fromkeys(product_categories))
            
            # Additional cleaning and validation
            cleaned_categories = []
            for category in product_categories:
                # Remove any markdown or unwanted characters
                cleaned_category = re.sub(r'[*_#`]', '', category).strip()
                if cleaned_category and len(cleaned_category) > 2:
                    cleaned_categories.append(cleaned_category)
            
            print(f"Found {len(cleaned_categories)} product categories for {brand_name}:", cleaned_categories)
            return {"brand_info": brand_info, "products": cleaned_categories}
            
        except Exception as e:
            print(f"Error getting brand info from Perplexity: {str(e)}")
            return {"brand_info": "", "products": []}

    def about_brand(self, brand_name: str, url: str) -> Dict:
        """Main method to analyze brand using multiple sources"""
        try:
            # Get brand info and products from Perplexity
            perplexity_data = self.get_brand_info_from_perplexity(brand_name)
            brand_info = perplexity_data["brand_info"]
            product_categories_list = perplexity_data["products"]  # Rename for clarity
            
            # Get website content using Jina API
            jina_url = f"https://r.jina.ai/{url}"
            jina_response = requests.get(jina_url)
            
            if jina_response.status_code != 200:
                error_message = f"Failed to fetch data from Jina API. Status code: {jina_response.status_code}"
                if jina_response.text:
                    error_message += f". Error details: {jina_response.text}"
                return jsonify({
                    "error": error_message,
                    "status": jina_response.status_code
                }), jina_response.status_code
            
            try:
                jina_data = jina_response.json()
            except json.JSONDecodeError:
                jina_data = jina_response.text
                
            content_str = str(jina_data)
            image_urls = re.findall(r'https?://[^\s<>"]+?(?:jpg|jpeg|gif|png|webp)', content_str, re.IGNORECASE)
        
            MAX_CONTENT_LENGTH = 12000
            if len(content_str) > MAX_CONTENT_LENGTH:
                content_str = content_str[:MAX_CONTENT_LENGTH] + "... (content truncated)"
            
            with open('website_content.json', 'w', encoding='utf-8') as f:
                json.dump({'content': content_str}, f, ensure_ascii=False, indent=4)
            prompt = f'''
            Brand Name: {brand_name}
            
            Detailed Brand Information:
            {brand_info}
            
            Only after complete analysis, provide the structured information in the format specified below.
            CRITICAL: You MUST fill out ALL fields with detailed information. Do not leave any field empty or with placeholder text.
            If specific information is not directly available, use the context to make a well-reasoned inference based on the brand's overall presentation and market position.

            IMPORTANT: For the brandLogo field, you must ONLY use the actual logo URL found in the website content. Look for image URLs containing terms like 'logo', 'brand', 'header-logo', etc. DO NOT make up or hallucinate URLs. If no logo URL is found, return an empty array.
            
            Website Content:
            {content_str}
            
            Based on the above information, provide a structured analysis in the following JSON format:
                {{
            "coreIdentity": {{
                "websiteUrl": "Main website URL found in the content",
                "brandName": "Primary brand or company name",
                "brandVision": "Long-term aspirational goal of the brand",
                "brandMission": "Primary purpose and objectives of the brand",
                "coreValues": ["List of 3-5 fundamental principles or beliefs that guide the brand"]
            }},
            "brandAsProduct": {{
                "productCategory": "Main product/service category",
                "qualityPosition": "Premium/High-Quality/Mid-Range/Value",
                "productAttributes": ["List of 3-5 key features or characteristics"],
                "keyBenefits": ["List of 3-5 primary customer benefits"]
            }},
            "brandAsPerson": {{
                "brandPersonality": ["2-4 traits: Sincere/Excited/Competent/Sophisticated/Rugged/Caring/Trustworthy/Innovative/Fun/Professional/Friendly/Bold"],
                "toneOfVoice": {{
                    "Professional": "high/medium/low",
                    "Friendly": "high/medium/low",
                    "Casual": "high/medium/low",
                    "Enthusiastic": "high/medium/low"
                }},
                "brandCustomerRelationship": ["1-3 types: Trusted Advisor/Friend/Expert Guide/Innovator/Support Partner/Mentor"]
            }},
            "brandAsSymbol": {{
                "brandColors": {{
                    "primary": "#HEX",
                    "secondary": "#HEX",
                    "accent": "#HEX"
                }},
                "fonts": ["Typography choices"],
                "visualStyle": ["2-3 styles: Minimalist/Bold/Classic/Modern/Playful/Elegant/Technical/Organic/Geometric"],
                "brandLogo": ["Only actual logo URLs"],
                "brandAssets": {image_urls}
            }}
        }}'''
            
            messages = [
                {
                    "role": "system", 
                    "content": "You are an expert market competitor researcher specializing. Generate focused, realistic competitor analysis data based on brand, product analysis and given data"
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ]
            
            response = requests.post(
                self.endpoint,
                headers=self.headers,
                json={
                    "messages": messages,
                    "temperature": 0.9,
                    "max_tokens": 4000,
                    "response_format": { "type": "json_object" }
                }
            )
            
            if response.status_code == 200:
                response_text = response.json()['choices'][0]['message']['content']
                analysis = json.loads(response_text)
                
                result = {
                    'status': 'success',
                    'brand_name': brand_name,
                    'brand_context': brand_info,
                    'product_categories': product_categories_list,  # Use consistent naming
                    'coreIdentity': analysis.get('coreIdentity', {}),
                    'brandAsProduct': analysis.get('brandAsProduct', {}),
                    'brandAsPerson': analysis.get('brandAsPerson', {}),
                    'brandAsSymbol': analysis.get('brandAsSymbol', {}),
                    'timestamp': datetime.now().isoformat()
                }
                # Add product categories to brandAsProduct for better structure
                result['brandAsProduct']['product_categories'] = product_categories_list
                return result
            else:
                print(f"Error from Azure OpenAI: {response.status_code}")
                print(f"Response: {response.text}")
                return {
                    'status': 'error',
                    'message': f"Azure OpenAI API error: {response.status_code}"
                }
                
        except Exception as e:
            print(f"Error in about_brand: {str(e)}")
            return {
                'status': 'error',
                'message': str(e)
            } 