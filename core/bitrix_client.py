"""
Bitrix24 API Client
Handles OAuth token refresh, file download, and field updates
"""
import os
import requests
from typing import Optional, Dict, Any
from utils.logger import setup_logger

logger = setup_logger("BitrixClient")


class BitrixClient:
    """Client for interacting with Bitrix24 API"""

    def __init__(self):
        self.domain = os.getenv("BITRIX_DOMAIN", "ua.zvilnymp.com.ua")
        self.oauth_url = os.getenv("BITRIX_OAUTH_URL", "https://oauth.bitrix.info/oauth/token/")
        self.client_id = os.getenv("BITRIX_CLIENT_ID")
        self.client_secret = os.getenv("BITRIX_CLIENT_SECRET")
        self.refresh_token = os.getenv("BITRIX_REFRESH_TOKEN")
        self.access_token: Optional[str] = None

        logger.info(f"BitrixClient initialized for domain: {self.domain}")

    def _refresh_access_token(self) -> str:
        """
        Refresh OAuth access token using refresh token

        Returns:
            New access token

        Raises:
            Exception if token refresh fails
        """
        try:
            logger.info("Refreshing Bitrix24 access token...")

            params = {
                "grant_type": "refresh_token",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "refresh_token": self.refresh_token
            }

            response = requests.get(self.oauth_url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()

            if "access_token" not in data:
                raise Exception(f"No access_token in response: {data}")

            self.access_token = data["access_token"]

            # Update refresh token if provided
            if "refresh_token" in data:
                self.refresh_token = data["refresh_token"]

            logger.info("Access token refreshed successfully")
            return self.access_token

        except Exception as e:
            logger.error(f"Failed to refresh access token: {e}")
            raise

    def get_access_token(self) -> str:
        """
        Get valid access token (refresh if needed)

        Returns:
            Valid access token
        """
        if not self.access_token:
            return self._refresh_access_token()
        return self.access_token

    def download_file_from_field(self, deal_id: int, field_code: str) -> bytes:
        """
        Download file from Bitrix24 deal field

        The field contains file info with downloadUrl
        Uses your schema: get downloadUrl from field, then download with auth token

        Args:
            deal_id: Deal ID
            field_code: Field code containing file (e.g., UF_CRM_1765540040027)

        Returns:
            File content as bytes

        Raises:
            Exception if download fails
        """
        try:
            logger.info(f"Downloading file from deal {deal_id}, field {field_code}")

            # Step 1: Get fresh access token
            token_response = requests.get(self.oauth_url, params={
                "grant_type": "refresh_token",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "refresh_token": self.refresh_token
            }, timeout=30)

            token_response.raise_for_status()
            token_data = token_response.json()

            if "access_token" not in token_data:
                raise Exception(f"No access_token in response: {token_data}")

            access_token = token_data["access_token"]
            logger.info("Access token obtained successfully")

            # Step 2: Get file field value from deal
            logger.info(f"Getting file info from field {field_code}")
            deal_url = f"https://{self.domain}/rest/crm.deal.get"
            deal_params = {
                "auth": access_token,
                "id": deal_id
            }

            deal_response = requests.get(deal_url, params=deal_params, timeout=30)
            deal_response.raise_for_status()
            deal_data = deal_response.json()

            if "result" not in deal_data:
                raise Exception(f"No result in deal response: {deal_data}")

            file_field = deal_data["result"].get(field_code)

            if not file_field:
                raise Exception(f"Field {field_code} is empty or not found")

            logger.info(f"File field value: {file_field}")

            # Step 3: Extract downloadUrl
            # File field can be a dict or array of dicts
            download_url = None

            if isinstance(file_field, dict):
                download_url = file_field.get("downloadUrl")
            elif isinstance(file_field, list) and len(file_field) > 0:
                download_url = file_field[0].get("downloadUrl")

            if not download_url:
                raise Exception(f"No downloadUrl found in field value: {file_field}")

            logger.info(f"Download URL extracted: {download_url}")

            # Step 4: Download file using your schema
            file_url = f"https://{self.domain}{download_url}&auth={access_token}"
            logger.info(f"Downloading from: {file_url}")

            file_response = requests.get(file_url, stream=True, timeout=60)
            file_response.raise_for_status()

            content = file_response.content
            logger.info(f"File downloaded successfully, size: {len(content)} bytes")

            return content

        except Exception as e:
            logger.error(f"Failed to download file from deal {deal_id}: {e}")
            raise

    def update_deal_field(self, deal_id: int, field_code: str, value: str) -> bool:
        """
        Update a field in Bitrix24 deal

        Args:
            deal_id: Deal ID
            field_code: Field code (e.g., UF_CRM_1765540114644)
            value: New field value

        Returns:
            True if successful

        Raises:
            Exception if update fails
        """
        try:
            logger.info(f"Updating deal {deal_id} field {field_code}")

            token = self.get_access_token()

            method_url = f"https://{self.domain}/rest/crm.deal.update"
            params = {
                "auth": token,
                "id": deal_id,
                "fields": {
                    field_code: value
                }
            }

            response = requests.post(method_url, json=params, timeout=30)

            # If unauthorized, refresh token and retry
            if response.status_code == 401:
                logger.warning("Access token expired, refreshing...")
                token = self._refresh_access_token()
                params["auth"] = token
                response = requests.post(method_url, json=params, timeout=30)

            response.raise_for_status()
            data = response.json()

            if not data.get("result"):
                raise Exception(f"Update failed: {data}")

            logger.info(f"Deal field updated successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to update deal {deal_id}: {e}")
            raise

    def add_timeline_comment(self, deal_id: int, comment: str) -> bool:
        """
        Add a comment to deal timeline

        Args:
            deal_id: Deal ID
            comment: Comment text (can include HTML)

        Returns:
            True if successful

        Raises:
            Exception if adding comment fails
        """
        try:
            logger.info(f"Adding timeline comment to deal {deal_id}")

            token = self.get_access_token()

            method_url = f"https://{self.domain}/rest/crm.timeline.comment.add"
            params = {
                "auth": token,
                "fields": {
                    "ENTITY_ID": deal_id,
                    "ENTITY_TYPE": "deal",
                    "COMMENT": comment
                }
            }

            response = requests.post(method_url, json=params, timeout=30)

            # If unauthorized, refresh token and retry
            if response.status_code == 401:
                logger.warning("Access token expired, refreshing...")
                token = self._refresh_access_token()
                params["auth"] = token
                response = requests.post(method_url, json=params, timeout=30)

            response.raise_for_status()
            data = response.json()

            if not data.get("result"):
                raise Exception(f"Adding comment failed: {data}")

            logger.info(f"Timeline comment added successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to add timeline comment to deal {deal_id}: {e}")
            raise

    def get_deal_field(self, deal_id: int, field_code: str) -> Any:
        """
        Get a field value from Bitrix24 deal

        Args:
            deal_id: Deal ID
            field_code: Field code to retrieve

        Returns:
            Field value

        Raises:
            Exception if retrieval fails
        """
        try:
            logger.info(f"Getting deal {deal_id} field {field_code}")

            token = self.get_access_token()

            method_url = f"https://{self.domain}/rest/crm.deal.get"
            params = {
                "auth": token,
                "id": deal_id
            }

            response = requests.get(method_url, params=params, timeout=30)

            # If unauthorized, refresh token and retry
            if response.status_code == 401:
                logger.warning("Access token expired, refreshing...")
                token = self._refresh_access_token()
                params["auth"] = token
                response = requests.get(method_url, params=params, timeout=30)

            response.raise_for_status()
            data = response.json()

            if "result" not in data:
                raise Exception(f"No result in response: {data}")

            value = data["result"].get(field_code)
            logger.info(f"Field value retrieved: {value}")

            return value

        except Exception as e:
            logger.error(f"Failed to get deal {deal_id} field {field_code}: {e}")
            raise
