import os
import time
import json
import logging
import requests
import google.generativeai as genai
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from dotenv import load_dotenv
import io
import boto3
from botocore.client import Config

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load Environment Variables
load_dotenv()

def get_env_var(name):
    val = os.environ.get(name)
    if not val:
        logger.error(f"Missing environment variable: {name}")
    return val

GOOGLE_CREDS_JSON_STR = get_env_var("GOOGLE_CREDS_JSON")
GEMINI_API_KEY = get_env_var("GEMINI_API_KEY")
MAKE_WEBHOOK_URL = get_env_var("MAKE_WEBHOOK_URL")

# Drive Folder IDs
ID_LINKEDIN = get_env_var("ID_LINKEDIN")
ID_META = get_env_var("ID_META")
ID_GBP = get_env_var("ID_GBP")
ID_ALL = get_env_var("ID_ALL")
ID_CONFIG = get_env_var("ID_CONFIG")
ID_PROCESSED = get_env_var("ID_PROCESSED")
ID_ERRORS = get_env_var("ID_ERRORS")

# R2 Configuration
R2_ACCOUNT_ID = get_env_var("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID = get_env_var("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = get_env_var("R2_SECRET_ACCESS_KEY")
R2_BUCKET_NAME = get_env_var("R2_BUCKET_NAME")

# Default Prompts
DEFAULT_PROMPT_LINKEDIN = "Write a professional, craftsmanship-focused LinkedIn caption for this image."
DEFAULT_PROMPT_META = "Write a casual, engaging Facebook/Instagram caption for this image."
DEFAULT_PROMPT_GBP = "Write an SEO-heavy Google Business Profile caption for this image with a 'Call us' CTA and no hashtags."

def get_drive_service():
    """Authenticates and returns the Google Drive API service."""
    try:
        creds_info = json.loads(GOOGLE_CREDS_JSON_STR)
        creds = service_account.Credentials.from_service_account_info(
            creds_info, scopes=['https://www.googleapis.com/auth/drive']
        )
        service = build('drive', 'v3', credentials=creds)
        return service
    except Exception as e:
        logger.error(f"Failed to initialize Google Drive service: {e}")
        return None

def setup_gemini():
    if GEMINI_API_KEY:
        genai.configure(api_key=GEMINI_API_KEY)
    else:
        logger.error("GEMINI_API_KEY is missing.")

def get_r2_client():
    """Initializes the R2 (S3) client."""
    try:
        return boto3.client('s3',
            endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
            aws_access_key_id=R2_ACCESS_KEY_ID,
            aws_secret_access_key=R2_SECRET_ACCESS_KEY,
            config=Config(signature_version='s3v4')
        )
    except Exception as e:
        logger.error(f"Failed to initialize R2 client: {e}")
        return None

def upload_to_r2(client, image_data, file_name, mime_type):
    """Uploads file to R2 and returns a presigned URL."""
    try:
        logger.info(f"Uploading {file_name} to R2...")
        client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=file_name,
            Body=image_data,
            ContentType=mime_type
        )
        # Generate a public URL valid for 1 hour
        url = client.generate_presigned_url(
            ClientMethod='get_object',
            Params={'Bucket': R2_BUCKET_NAME, 'Key': file_name},
            ExpiresIn=3600
        )
        return url
    except Exception as e:
        logger.error(f"R2 Upload failed: {e}")
        return None

def get_text_content(service, file_id):
    try:
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        return fh.getvalue().decode('utf-8').strip()
    except Exception as e:
        logger.warning(f"Failed to read prompt file {file_id}: {e}")
        return None

def get_prompts(service):
    prompts = {
        "linkedin": DEFAULT_PROMPT_LINKEDIN,
        "meta": DEFAULT_PROMPT_META,
        "gbp": DEFAULT_PROMPT_GBP
    }
    if not service or not ID_CONFIG:
        return prompts
    try:
        query = f"'{ID_CONFIG}' in parents and trashed = false and mimeType = 'text/plain'"
        results = service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get('files', [])
        for file in files:
            name = file.get('name').lower()
            if name == 'prompt_linkedin.txt':
                content = get_text_content(service, file.get('id'))
                if content: prompts["linkedin"] = content
            elif name == 'prompt_meta.txt':
                content = get_text_content(service, file.get('id'))
                if content: prompts["meta"] = content
            elif name == 'prompt_gbp.txt':
                content = get_text_content(service, file.get('id'))
                if content: prompts["gbp"] = content
    except Exception as e:
        logger.error(f"Error fetching prompts: {e}")
    return prompts

def generate_caption(image_data, mime_type, prompt):
    try:
        model = genai.GenerativeModel(
            "gemini-2.0-flash",
            system_instruction="You are a social media engine. Output ONLY the caption. Do not output conversational filler."
        )
        content_parts = [{"mime_type": mime_type, "data": image_data}, prompt]
        response = model.generate_content(content_parts)
        return response.text.strip()
    except Exception as e:
        logger.error(f"Gemini generation failed: {e}")
        raise e

def move_file(service, file_id, destination_folder_id):
    try:
        file = service.files().get(fileId=file_id, fields='parents').execute()
        previous_parents = ",".join(file.get('parents'))
        service.files().update(
            fileId=file_id,
            addParents=destination_folder_id,
            removeParents=previous_parents,
            fields='id, parents'
        ).execute()
        logger.info(f"Moved file {file_id} to {destination_folder_id}")
    except Exception as e:
        logger.error(f"Failed to move file {file_id}: {e}")

def process_file(service, r2_client, file_info, source_type, prompts):
    file_id = file_info['id']
    file_name = file_info['name']
    mime_type = file_info['mimeType']
    
    logger.info(f"Processing file {file_name} from {source_type}")
    
    try:
        # Download Image from Drive
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        image_data = fh.getvalue()
        
        payload = {}
        
        # 1. Generate Captions based on Source
        if source_type == "linkedin":
            payload["caption_linkedin"] = generate_caption(image_data, mime_type, prompts["linkedin"])
            payload["target"] = "linkedin"
        elif source_type == "meta":
            payload["caption_meta"] = generate_caption(image_data, mime_type, prompts["meta"])
            payload["target"] = "meta"
        elif source_type == "gbp":
            payload["caption_gbp"] = generate_caption(image_data, mime_type, prompts["gbp"])
            payload["target"] = "gbp"
        elif source_type == "all":
            payload["caption_linkedin"] = generate_caption(image_data, mime_type, prompts["linkedin"])
            payload["caption_meta"] = generate_caption(image_data, mime_type, prompts["meta"])
            payload["caption_gbp"] = generate_caption(image_data, mime_type, prompts["gbp"])
            payload["target"] = "all"

        # 2. Upload to R2 (Only if Instagram needs it)
        # Instagram is triggered by 'meta' or 'all'
        if source_type in ["meta", "all"] and r2_client:
            public_url = upload_to_r2(r2_client, image_data, file_name, mime_type)
            if public_url:
                payload["image_url"] = public_url
        
        # 3. Send Webhook
        # We send BOTH the binary file (for FB/LinkedIn) and the URL (for Instagram)
        files = {
            'file': (file_name, image_data, mime_type)
        }
        
        logger.info(f"Sending webhook for {file_name}...")
        response = requests.post(MAKE_WEBHOOK_URL, data=payload, files=files)
        response.raise_for_status()
        
        logger.info(f"Webhook success: {response.status_code}")
        
        # 4. Move to Processed
        if ID_PROCESSED:
            move_file(service, file_id, ID_PROCESSED)
            
    except Exception as e:
        logger.error(f"Error processing file {file_name}: {e}")
        if ID_ERRORS:
            move_file(service, file_id, ID_ERRORS)

def main():
    logger.info("Starting Kolmo Social Engine...")
    setup_gemini()
    
    service = get_drive_service()
    if not service:
        logger.critical("Could not initialize Drive Service. Exiting.")
        return

    r2_client = get_r2_client()
    if not r2_client:
        logger.warning("Could not initialize R2 Client. Instagram uploads may fail.")

    folder_map = {
        ID_LINKEDIN: "linkedin",
        ID_META: "meta",
        ID_GBP: "gbp",
        ID_ALL: "all"
    }

    while True:
        try:
            logger.info("Starting poll cycle...")
            current_prompts = get_prompts(service)
            
            for folder_id, source_type in folder_map.items():
                if not folder_id: continue
                
                query = f"'{folder_id}' in parents and trashed = false and mimeType contains 'image/'"
                results = service.files().list(q=query, fields="files(id, name, mimeType)").execute()
                items = results.get('files', [])
                
                if items:
                    logger.info(f"Found {len(items)} images in {source_type} folder.")
                    for item in items:
                        process_file(service, r2_client, item, source_type, current_prompts)
            
            logger.info("Cycle complete. Sleeping for 60s...")
            time.sleep(60)
            
        except KeyboardInterrupt:
            logger.info("Stopping...")
            break
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
            time.sleep(60)

if __name__ == "__main__":
    main()