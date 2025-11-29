# Kolmo Social Engine

A serverless Python automation service that polls multiple Google Drive folders, generates social media captions using Google Gemini 2.0 Flash, and sends the data to a Make.com webhook.

## Project Structure

- `main.py`: Core logic for polling, prompting, and routing.
- `requirements.txt`: Python dependencies.
- `.env`: Environment variables (not included in repo).

## Configuration

Required Environment Variables:

- `GOOGLE_CREDS_JSON`: Service Account key (raw JSON string).
- `GEMINI_API_KEY`: Google AI Studio API Key.
- `MAKE_WEBHOOK_URL`: Destination Webhook URL.
- `ID_LINKEDIN`, `ID_META`, `ID_GBP`, `ID_ALL`: Google Drive Folder IDs for input.
- `ID_CONFIG`: Folder ID containing prompt text files.
- `ID_PROCESSED`: Archive Folder ID.
- `ID_ERRORS`: Error Quarantine Folder ID.

## Usage

Run the script:

```bash
python main.py
```

The service runs in an infinite loop, polling every 60 seconds.
