import requests
import datetime
from bs4 import BeautifulSoup

def download_data_dolar(url, current_date):
    '''
    Download and parse data from a webpage containing dollar exchange rates.

    Args:
        url (str): The URL of the webpage to scrape.
        current_date (str): The current date to include in the result data.

    Returns:
        list of dict: A list of dictionaries where each dictionary represents a row from the table.
                      Each dictionary contains the bank name, the current date, and either 'otro',
                      'compra', or 'venta' values depending on the number of columns in the row.

    Raises:
        requests.exceptions.HTTPError: If the HTTP request to the webpage fails (status code != 200).
        ValueError: If no <tbody> element is found in the webpage.
    '''

    # Send a GET request to the specified URL
    response = requests.get(url)
    
    # Check if the request was successful (status code 200)
    if response.status_code != 200:
        # Raise an HTTPError if the request was not successful
        raise requests.exceptions.HTTPError(f'Failed to retrieve the webpage. Status code: {response.status_code}')

    # Parse the webpage content with BeautifulSoup using the HTML parser
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find the <tbody> element which contains the table data
    tbody = soup.find('tbody')
    if not tbody:
        # Raise a ValueError if no <tbody> element is found
        raise ValueError('No <tbody> element found on the page.')
    
    # Initialize lists to hold the extracted table data and headers
    data = []
    headers = []

    # Find all rows (<tr> elements) within the <tbody> element
    rows = tbody.find_all('tr')
    
    # Extract the header titles from the first row
    titles = [row.find_all('td') for row in rows]
    headers = [title[0].find_all('span', title=True)[0].get('title') for title in titles]
    
    # Loop through the remaining rows to extract the data
    for row in rows:
        # Find all cells (<td> elements) in the current row
        cells = row.find_all('td')
        # Get the text from each cell, stripping extra whitespace
        row_data = [cell.get_text(strip=True) for cell in cells]
        # Append the extracted row data to the data list
        data.append(row_data)
    
    # Convert the extracted data into a list of dictionaries
    # Each dictionary maps headers to corresponding cell data in each row
    result = [{'banco': header, 
               'date': current_date, 
               **({'otro': row[-1]} if len(row) == 4 else {'compra': row[-2], 'venta': row[-1]})} 
              for header, row in zip(headers, data)]
    
    return result

url = 'https://www.eldolar.info/es-MX/mexico/dia/'

data = []

start_date_str = '20140401'
start_date = datetime.datetime.strptime(start_date_str, '%Y%m%d')

# Get the current date and time
end_date = datetime.datetime.now()

# Print each date from the start date to the current date
current_date = start_date
while current_date <= end_date:
    # Print the date in YYYY-MM-DD format
    data.append(download_data_dolar( url + current_date.strftime('%Y%m%d'), current_date))
    # Increment the date by one day
    current_date += datetime.timedelta(days=1)
