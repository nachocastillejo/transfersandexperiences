"""
Supabase Storage service for uploading and managing media files.
"""
import os
import logging
import requests
from typing import Optional
from flask import current_app


def upload_file_to_storage(file_path: str, destination_path: str, mime_type: str) -> Optional[str]:
    """
    Upload a file to Supabase Storage and return the public URL.
    
    Args:
        file_path: Local path to the file to upload
        destination_path: Path in storage bucket (e.g., "2024/11/image.jpg")
        mime_type: MIME type of the file
        
    Returns:
        Public URL of the uploaded file, or None if upload failed
    """
    try:
        supabase_url = os.getenv('SUPABASE_URL')
        service_role_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
        
        if not supabase_url or not service_role_key:
            logging.error("Supabase credentials not configured")
            return None
        
        bucket_name = 'media-files'
        
        # Construct the storage API URL
        storage_url = f"{supabase_url}/storage/v1/object/{bucket_name}/{destination_path}"
        
        headers = {
            'Authorization': f'Bearer {service_role_key}',
            'Content-Type': mime_type,
            'x-upsert': 'true'  # Overwrite if exists
        }
        
        # Read and upload the file
        with open(file_path, 'rb') as f:
            file_data = f.read()
        
        response = requests.post(storage_url, headers=headers, data=file_data, timeout=30)
        
        if response.status_code in [200, 201]:
            # Construct public URL
            public_url = f"{supabase_url}/storage/v1/object/public/{bucket_name}/{destination_path}"
            logging.info(f"File uploaded successfully to Storage: {public_url}")
            return public_url
        else:
            logging.error(f"Failed to upload to Supabase Storage: {response.status_code} {response.text}")
            return None
            
    except Exception as e:
        logging.error(f"Error uploading file to Supabase Storage: {e}", exc_info=True)
        return None


def delete_file_from_storage(storage_path: str) -> bool:
    """
    Delete a file from Supabase Storage.
    
    Args:
        storage_path: Path in storage bucket (e.g., "2024/11/image.jpg")
        
    Returns:
        True if deletion was successful, False otherwise
    """
    try:
        supabase_url = os.getenv('SUPABASE_URL')
        service_role_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
        
        if not supabase_url or not service_role_key:
            logging.error("Supabase credentials not configured")
            return False
        
        bucket_name = 'media-files'
        
        # Construct the storage API URL
        storage_url = f"{supabase_url}/storage/v1/object/{bucket_name}/{storage_path}"
        
        headers = {
            'Authorization': f'Bearer {service_role_key}'
        }
        
        response = requests.delete(storage_url, headers=headers, timeout=10)
        
        if response.status_code in [200, 204]:
            logging.info(f"File deleted successfully from Storage: {storage_path}")
            return True
        else:
            logging.warning(f"Failed to delete from Supabase Storage: {response.status_code} {response.text}")
            return False
            
    except Exception as e:
        logging.error(f"Error deleting file from Supabase Storage: {e}", exc_info=True)
        return False

