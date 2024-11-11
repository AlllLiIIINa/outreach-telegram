import logging
import os
import re
from datetime import datetime
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

load_dotenv()

# Настройки для Google Sheets
SERVICE_ACCOUNT_FILE = os.environ.get("SERVICE_ACCOUNT_FILE")
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/drive.file'
]
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)
sheets_service = build('sheets', 'v4', credentials=credentials)
drive_service = build('drive', 'v3', credentials=credentials)


class GoogleSheetsHandler:
    def __init__(self, sheets_service, drive_service):
        self.sheets_service = sheets_service
        self.drive_service = drive_service
        self.user_spreadsheets = {}  # Cache for storing user spreadsheet IDs

    async def find_user_spreadsheet(self, username):
        """Find existing spreadsheet for user"""
        try:
            # Search for a file by name
            query = f"name = 'Results for {username}'"
            results = self.drive_service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)'
            ).execute()
            files = results.get('files', [])

            if files:
                return files[0]['id']
            return None

        except HttpError as error:
            logging.error(f'Error finding spreadsheet: {error}')
            return None

    async def get_or_create_user_spreadsheet(self, user_id, username):
        """Get existing or create new spreadsheet for user"""
        # First check the cache
        if user_id in self.user_spreadsheets:
            return self.user_spreadsheets[user_id]

        # If not in the cache, look for an existing table
        existing_spreadsheet_id = await self.find_user_spreadsheet(username)
        if existing_spreadsheet_id:
            self.user_spreadsheets[user_id] = existing_spreadsheet_id
            return existing_spreadsheet_id

        # If the table is not found, create a new one
        spreadsheet = {
            'properties': {
                'title': f'Results for {username}'
            }
        }

        try:
            spreadsheet = self.sheets_service.spreadsheets().create(
                body=spreadsheet,
                fields='spreadsheetId'
            ).execute()
            spreadsheet_id = spreadsheet.get('spreadsheetId')
            self.user_spreadsheets[user_id] = spreadsheet_id

            # Grant access to specified email
            permission = {
                'type': 'user',
                'role': 'writer',
                'emailAddress': 'alina.tvvv@gmail.com'
            }
            self.drive_service.permissions().create(
                fileId=spreadsheet_id,
                body=permission
            ).execute()

            logging.info(f'Created new spreadsheet for user {username} (ID: {spreadsheet_id})')
            return spreadsheet_id

        except HttpError as error:
            logging.error(f'Error creating spreadsheet: {error}')
            raise

    async def create_sheet_for_query(self, spreadsheet_id, query_details, data):
        """Create new sheet for specific search query and format data with WhatsApp links"""
        row = ""

        try:
            # Extract query details
            category = query_details.get("category", "Unknown")
            country = query_details.get("country", "Unknown")
            city = query_details.get("city", "Unknown")

            # Generate sheet name
            date_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            sheet_name = f"{category}-{country}-{city}-{date_str}"[:50]

            # Create new sheet
            requests = [{
                "addSheet": {
                    "properties": {
                        "title": sheet_name,
                        "gridProperties": {
                            "rowCount": 1000,
                            "columnCount": 10
                        }
                    }
                }
            }]

            body = {'requests': requests}
            self.sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=body
            ).execute()

            # Headers
            headers = ['Source', 'Company Name', 'Website', 'Email', 'Phone', 'WhatsApp Link',
                       'Location', 'Rating', 'Reviews', 'Verification']
            values = [headers]

            def create_whatsapp_link(phone_num):
                if not phone_num or phone_num == 'N/A':
                    return 'N/A'

                digits = ''.join(re.findall(r'\d+', phone_num))  # Extract only digits from phone number
                if len(digits) >= 8:  # Check if we have enough digits for a valid phone number
                    return f"https://wa.me/{digits}"
                return 'N/A'

            # Process the data
            for source, company_data in data:
                logging.info(f"Processing item - Source: {source}, Data: {company_data}")

                if source == 'TrustPilot' and isinstance(company_data, tuple):
                    # Handle TrustPilot data
                    name, website, emails, phone, location, rating, reviews, verify = company_data + ('N/A',) * (
                            8 - len(company_data))
                    emails_str = ', '.join(emails) if isinstance(emails, list) else str(emails)
                    whatsapp_link = create_whatsapp_link(phone)

                    # Format all fields
                    name, website, emails_str, phone, whatsapp_link, location, rating, reviews, verify = (
                        str(x).strip() for x in (
                            name, website, emails_str, phone, whatsapp_link, location, rating, reviews, verify
                        )
                    )
                    row = [source, name, website, emails_str, phone, whatsapp_link,
                           location, rating, reviews, verify]

                elif source == 'Google Maps' and isinstance(company_data, tuple):
                    # Handle Google Maps data
                    name, website, emails, phone, location, reviews = company_data + ('N/A',) * (6 - len(company_data))
                    emails_str = ', '.join(emails) if isinstance(emails, list) else str(emails)
                    whatsapp_link = create_whatsapp_link(phone)

                    # Format all fields
                    name, website, emails_str, phone, whatsapp_link, location, reviews = (
                        str(x).strip() for x in (
                            name, website, emails_str, phone, whatsapp_link, location, reviews
                        )
                    )
                    row = [source, name, website, emails_str, phone, whatsapp_link,
                           location, 'N/A', reviews, 'N/A']

                values.append(row)

            # Write the data to the sheet
            range_name = f"{sheet_name}!A1:J{len(values)}"
            body = {
                'values': values
            }
            self.sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption='RAW',
                body=body
            ).execute()

            return sheet_name

        except Exception as e:
            logging.error(f"Error creating sheet: {str(e)}")
            raise


async def create_google_sheet(message, data, query_details):
    """Main function to handle sheet creation"""
    sheets_handler = GoogleSheetsHandler(sheets_service, drive_service)

    try:
        # Get or create user's spreadsheet
        spreadsheet_id = await sheets_handler.get_or_create_user_spreadsheet(
            message.from_user.id,
            message.from_user.username or str(message.from_user.id)
        )
        # Create new sheet for this search query
        await sheets_handler.create_sheet_for_query(spreadsheet_id, query_details, data)

        return spreadsheet_id

    except Exception as e:
        logging.error(f"Error in create_google_sheet: {e}")
        raise
