# Creative Extraction API

A Flask-based API for extracting, analyzing, and storing creative content from Meta ads using AI-powered analysis.

## Features

- Extract creative content (images, videos) from Meta advertisements
- Analyze creative content using Google's Gemini AI
- Store analysis results in MongoDB and Elasticsearch
- Process ads in batches for efficient handling
- Detailed extraction of ad elements including hooks, themes, keywords, and more
- Brand analysis for comprehensive insights

## Requirements

- Python 3.8+
- MongoDB Atlas account
- Google Gemini API key
- Elasticsearch (optional, for advanced search capabilities)

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/ExtractEntities.git
   cd ExtractEntities
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Set up environment variables by copying the example file:
   ```bash
   cp .env.example .env
   ```
   
5. Edit the `.env` file with your credentials:
   ```
   # MongoDB Configuration
   MONGO_URI=mongodb+srv://username:password@cluster0.example.mongodb.net/
   
   # Gemini API Key
   GEMINI_API_KEY=your_gemini_api_key
   
   # Elasticsearch Configuration (optional)
   ELASTICSEARCH_CLOUD_ID=your_elasticsearch_cloud_id
   ELASTICSEARCH_API_KEY=your_elasticsearch_api_key
   ELASTICSEARCH_IMAGE_INDEX=image_competitors_analysis
   ELASTICSEARCH_VIDEO_INDEX=video_competitors_analysis
   ```

## Usage

### Starting the API

Run the application:
```bash
python creative_extraction.py
```

The API will be available at `http://localhost:5000`.

### API Endpoints

#### Analyze Creatives

**Endpoint**: `/analyze`

**Method**: POST

**Request Body**:
```json
{
  "page_id": "YOUR_PAGE_ID",
  "url": "https://your-brand-url.com"
}
```

- `page_id`: The Meta page ID to analyze
- `url`: (Optional) URL of the brand's website for additional context

**Example Request**:
```bash
curl -X POST http://localhost:5000/analyze \
  -H "Content-Type: application/json" \
  -d '{"page_id": "106729994530632", "url": "https://www.ccbp.in/"}'
```

**Response**:
```json
{
  "success": true,
  "page_id": "106729994530632",
  "url": "https://www.ccbp.in/",
  "total_ads": 50,
  "processed_ads": 45,
  "failed_ads": 5
}
```

## Error Handling

The API returns appropriate HTTP status codes:
- `400`: Bad request (missing required parameters)
- `404`: Not found (no ads data available)
- `500`: Internal server error (error during processing)

## Data Storage

- **MongoDB**: Stores detailed creative analysis and ad metadata
- **Elasticsearch** (optional): Enables advanced searching and filtering of creative content

## Utility Scripts

The repository includes additional utility scripts:
- `delete_meta_ads.py`: Tool to delete specific Meta ads data
- `brand_analyser.py`: Module for comprehensive brand analysis

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. 