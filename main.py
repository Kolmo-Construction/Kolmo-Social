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
        # We might want to exit or raise depending on severity, 
        # but for now we'll just log error and return None which might cause fail later.
    return val

GOOGLE_CREDS_JSON_STR = get_env_var("GOOGLE_CREDS_JSON")
GEMINI_API_KEY = get_env_var("GEMINI_API_KEY")
MAKE_WEBHOOK_URL = get_env_var("MAKE_WEBHOOK_URL")

ID_LINKEDIN = get_env_var("ID_LINKEDIN")
ID_META = get_env_var("ID_META")
ID_GBP = get_env_var("ID_GBP")
ID_ALL = get_env_var("ID_ALL")
ID_CONFIG = get_env_var("ID_CONFIG")
ID_PROCESSED = get_env_var("ID_PROCESSED")
ID_ERRORS = get_env_var("ID_ERRORS")

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
    """Configures the Gemini API."""
    if GEMINI_API_KEY:
        genai.configure(api_key=GEMINI_API_KEY)
    else:
        logger.error("GEMINI_API_KEY is missing.")

def get_text_content(service, file_id):
    """Downloads and returns text content from a Drive file."""
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
    """Fetches prompts from ID_CONFIG folder or returns defaults."""
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
    """Generates a caption using Gemini 2.0 Flash."""
    try:
        model = genai.GenerativeModel(
            "gemini-2.0-flash",
            system_instruction="You are a social media engine. Output ONLY the caption. Do not output conversational filler."
        )
        
        content_parts = [
            {"mime_type": mime_type, "data": image_data},
            prompt
        ]
        
        response = model.generate_content(content_parts)
        return response.text.strip()
    except Exception as e:
        logger.error(f"Gemini generation failed: {e}")
        raise e

def move_file(service, file_id, destination_folder_id):
    """Moves a file to a new folder."""
    try:
        # Retrieve the existing parents to remove
        file = service.files().get(fileId=file_id, fields='parents').execute()
        previous_parents = ",".join(file.get('parents'))
        
        # Move the file by adding the new parent and removing the old one
        service.files().update(
            fileId=file_id,
            addParents=destination_folder_id,
            removeParents=previous_parents,
            fields='id, parents'
        ).execute()
        logger.info(f"Moved file {file_id} to {destination_folder_id}")
    except Exception as e:
        logger.error(f"Failed to move file {file_id}: {e}")

def process_file(service, file_info, source_type, prompts):
    """Downloads file, generates caption(s), sends webhook, and moves file."""
    file_id = file_info['id']
    file_name = file_info['name']
    mime_type = file_info['mimeType']
    
    logger.info(f"Processing file {file_name} from {source_type}")
    
    try:
        # Download Image
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        image_data = fh.getvalue()
        
        payload = {}
        
        # Determine Logic based on Source
        if source_type == "linkedin":
            caption = generate_caption(image_data, mime_type, prompts["linkedin"])
            payload["caption_linkedin"] = caption
            payload["target"] = "linkedin"
            
        elif source_type == "meta":
            caption = generate_caption(image_data, mime_type, prompts["meta"])
            payload["caption_meta"] = caption
            payload["target"] = "meta"
            
        elif source_type == "gbp":
            caption = generate_caption(image_data, mime_type, prompts["gbp"])
            payload["caption_gbp"] = caption
            payload["target"] = "gbp"
            
        elif source_type == "all":
            payload["caption_linkedin"] = generate_caption(image_data, mime_type, prompts["linkedin"])
            payload["caption_meta"] = generate_caption(image_data, mime_type, prompts["meta"])
            payload["caption_gbp"] = generate_caption(image_data, mime_type, prompts["gbp"])
            payload["target"] = "all"
        
        # Send Webhook
        files = {
            'file': (file_name, image_data, mime_type)
        }
        
        logger.info(f"Sending webhook for {file_name}...")
        response = requests.post(MAKE_WEBHOOK_URL, data=payload, files=files)
        response.raise_for_status()
        
        logger.info(f"Webhook success: {response.status_code}")
        
        # Success: Move to Processed
        if ID_PROCESSED:
            move_file(service, file_id, ID_PROCESSED)
            
    except Exception as e:
        logger.error(f"Error processing file {file_name}: {e}")
        # Failure: Move to Errors
        if ID_ERRORS:
            move_file(service, file_id, ID_ERRORS)

def main():
    logger.info("Starting Kolmo Social Engine...")
    setup_gemini()
    
    service = get_drive_service()
    if not service:
        logger.critical("Could not initialize Drive Service. Exiting.")
        return

    # Folder Map
    folder_map = {
        ID_LINKEDIN: "linkedin",
        ID_META: "meta",
        ID_GBP: "gbp",
        ID_ALL: "all"
    }

    while True:
        try:
            logger.info("Starting poll cycle...")
            
            # Hot-reload prompts
            current_prompts = get_prompts(service)
            logger.debug(f"Prompts loaded: {current_prompts.keys()}")

            # Iterate through watched folders
            for folder_id, source_type in folder_map.items():
                if not folder_id:
                    continue
                
                query = f"'{folder_id}' in parents and trashed = false and mimeType contains 'image/'"
                results = service.files().list(q=query, fields="files(id, name, mimeType)").execute()
                items = results.get('files', [])
                
                if items:
                    logger.info(f"Found {len(items)} images in {source_type} folder.")
                    for item in items:
                        process_file(service, item, source_type, current_prompts)
            
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
