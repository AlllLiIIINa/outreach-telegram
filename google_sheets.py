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
            # Поиск файла по названию
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
        """Create new sheet for specific search query"""
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
                            "columnCount": 9
                        }
                    }
                }
            }]

            body = {'requests': requests}
            self.sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=body
            ).execute()

            # Prepare data for writing
            headers = ['Source', 'Company Name', 'Website', 'Email', 'Phone', 'Location', 'Rating', 'Reviews',
                       'Verification']
            values = [headers]

            # Process the data
            for source, company_data in data:
                logging.info(f"Processing item - Source: {source}, Data: {company_data}")

                if source == 'TrustPilot' and isinstance(company_data, tuple):
                    # Handle TrustPilot data
                    name, website, emails, phone, location, rating, reviews, verify = company_data + ('N/A',) * (
                            8 - len(company_data))
                    emails_str = ', '.join(emails) if isinstance(emails, list) else str(emails)
                    # wa_phone = f"https://wa.me/{''.join(re.findall(r'\d', phone))}"
                    name, website, emails_str,  phone, location, rating, reviews, verify = (
                        str(x).ljust(len(str(x))) for x in (
                            name, website, emails_str, phone, location, rating, reviews, verify
                        )
                    )
                    row = [source, name, website, emails_str, phone, location, rating, reviews, verify]

                elif source == 'Google Maps' and isinstance(company_data, tuple):
                    # Handle Google Maps data
                    name, website, emails, phone, location, reviews = company_data + ('N/A',) * (6 - len(company_data))
                    emails_str = ', '.join(emails) if isinstance(emails, list) else str(emails)
                    # wa_phone = f"https://wa.me/{''.join(re.findall(r'\d', phone))}"
                    name, website, emails_str, phone, location, reviews = (
                        str(x).ljust(len(str(x))) for x in (
                            name, website, emails_str, phone, location, reviews
                        )
                    )
                    row = [source, name, website, emails_str, phone, location, 'N/A', reviews, 'N/A']

                elif isinstance(company_data, dict):
                    # Handle dictionary format (legacy support)
                    name = str(company_data.get('name', 'N/A')).ljust
                    website = str(company_data.get('site', 'N/A')).ljust
                    emails = str(company_data.get('email', 'N/A')).ljust
                    phone = str(company_data.get('phone', 'N/A')).ljust
                    wa_phone = f"https://wa.me/{''.join(re.findall(r'\d', phone))}"
                    location = str(company_data.get('location', 'N/A')).ljust
                    reviews = str(company_data.get('reviews', 'N/A')).ljust
                    verify = str(company_data.get('verification', 'N/A')).ljust

                    if source == 'Google Maps':
                        row = [source, name, website, emails, phone, location, 'N/A', reviews, verify]
                    else:  # TrustPilot
                        rating = str(company_data.get('rating', 'N/A'))
                        row = [source, name, website, emails, phone, location, rating, reviews, verify]

                else:
                    logging.warning(f"Unexpected data format: {type(company_data)}")
                    continue

                values.append(row)

            # Write data to sheet
            range_name = f'{sheet_name}!A1:I{len(values)}'
            body = {'values': values}
            logging.info(f"Writing data to sheet. Range: {range_name}, Values count: {len(values)}")

            result = self.sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption='RAW',
                body=body
            ).execute()

            logging.info(
                f'Created new sheet "{sheet_name}" and wrote {len(values) - 1} rows of data. Update result: {result}')

        except HttpError as error:
            logging.error(f'Error creating sheet: {error}')
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
