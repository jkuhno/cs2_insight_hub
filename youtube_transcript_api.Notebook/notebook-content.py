# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6bf3ae1d-83fa-4bfa-916d-b68e8dae4662",
# META       "default_lakehouse_name": "CsHouse",
# META       "default_lakehouse_workspace_id": "2f4df65b-a3e2-423f-be79-386e691d4213",
# META       "known_lakehouses": [
# META         {
# META           "id": "6bf3ae1d-83fa-4bfa-916d-b68e8dae4662"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "537ae0e5-05c3-4d9b-8a29-9d1847646510",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Youtube transcripts api - Notebook
# is notebook downloads transcripts from selected Youtube videos. Two categories are used here: highlights and podcasts. The notebook downloads the transcript to either one of the folders based on the "video_ids" category.
# e Youtube transcripts python API allows you to get the transcript/subtitles for a given YouTube video, where transcripts are enabled. More info can be found from here: https://github.com/jdepoix/youtube-transcript-api/tree/master
# o run without using a proxy service, remove ", proxies=proxy" from "transcript = YouTubeTranscriptApi.get_transcript(video_id, languages=[language], proxies=proxy)" on the line 48*.
# n that case, copy the script to a local environment and manually upload the files to the lakehouse, and make the directory structure in the lakehouse:*
# Files
#   - transcripts
#       - podcasts
#       - highlights
# HOW TO ADD VIDEOS TO THIS NOTEBOOK:**
#  Select the Youtube video
#  Copy the video ID (youtube[.]com/watch?v=[VIDEO ID])
#  Paste the video ID to the "video_ids" values.
# uthors: Joel kataja, Jani Kuhno_


# CELL ********************

from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled
import os

# Dictionary of YouTube video IDs categorized into "highlights" and "podcasts"
# Replace with your specific video IDs if needed.
video_ids = {
    "highlights": ["WDYPur2Yz4o", "j5gMkwGPUZQ", "mmEF0rzf7o8", "PfsXFQDszdw", "rolF-81xbAQ", "hqhF915Vfmk"],
    "podcasts": ["ao_ey3wIEbI&list", "EBlajCO5f7Q", "f9gYX_QfYCo", "Rx3Ft3LjG04", "3F1k07uyBhE", "s0-Ig4QFvUs"]
}

# Define the path to the parent folder
folder_path = '/lakehouse/default/Files/transcripts'

# Required folders
required_folders = ['podcasts', 'highlights']

# Check and create missing folders
for folder in required_folders:
    folder_full_path = os.path.join(folder_path, folder)
    
    if not os.path.exists(folder_full_path):
        print(f"'{folder}' folder is missing. Creating it...")
        os.makedirs(folder_full_path)
    else:
        print(f"'{folder}' folder already exists")

# Proxy settings are necessary because YouTube blocks requests from Microsoft Fabric IP addresses.
# In this example, we're using a free proxy from Web Share. Replace with your own proxy credentials as needed.
proxyuser = "your-proxy-user"
proxycred = "your-proxy-credential"
proxy = {
    "https": f"http://{proxyuser}:{proxycred}@167.160.180.203:6754"
}

def fetch_auto_transcript(video_id, language='en'):
    """
    Fetches the transcript for a given YouTube video in the specified language.

    Args:
        video_id (str): The YouTube video ID to fetch the transcript for.
        language (str): Language code for the transcript. Defaults to 'en' (English).

    Returns:
        str or None: Transcript text as a single string, or None if fetching fails.
    """
    try:
        # Attempt to retrieve the transcript using specified language and proxy
        transcript = YouTubeTranscriptApi.get_transcript(video_id, languages=[language], proxies=proxy)
    except TranscriptsDisabled:
        print(f"Transcripts are disabled for video ID {video_id}. Possibly IP-blocked.")
        return None
    except Exception as e:
        print(f"Error fetching transcript for video ID {video_id}: {e}")
        return None

    # Concatenate the text of all transcript entries into a single string
    transcript_text = " ".join([entry['text'] for entry in transcript])
    return transcript_text

def write_as_text(fname, text):
    """
    Saves a given text to a file with specified filename.

    Args:
        fname (str): The full path and filename where the text will be saved.
        text (str): The text content to save in the file.
    """
    with open(fname, "w", encoding="utf-8") as file:
        file.write(text)
    print(f"Transcript saved to {fname}")

# Main script execution: iterate over video categories and their respective video IDs
for category, ids in video_ids.items():
    for video_id in ids:
        # Fetch transcript for each video ID
        transcript_text = fetch_auto_transcript(video_id)
        
        if transcript_text:  # Only proceed if transcript was successfully fetched
            # Construct filename using category and video ID
            fname = f"{folder_path}/{category}/{category}_{video_id}.txt"
            # Save the transcript to the file
            write_as_text(fname, transcript_text)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
