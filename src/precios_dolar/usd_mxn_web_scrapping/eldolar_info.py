import requests
import datetime
from bs4 import BeautifulSoup
import pandas as pd

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

def split_dict(data):
    """
    Splits a list of dictionaries into three separate dictionaries based on the presence of the keys 'compra', 'venta', and 'otro'.

    Each dictionary in the input list is expected to have the following structure:
    - A 'banco' key with the bank name as its value.
    - A 'date' key with a datetime object representing the date.
    - Optionally, one or more of the following keys: 'compra', 'venta', 'otro', each containing a value.

    The function returns three dictionaries:
    - `compra_dict`: Contains entries with the 'compra' key.
    - `venta_dict`: Contains entries with the 'venta' key.
    - `otro_dict`: Contains entries with the 'otro' key.

    Args:
        data (list of dict): List of dictionaries, where each dictionary contains data for a specific bank on a given date.

    Returns:
        tuple: A tuple containing three dictionaries:
            - `compra_dict` (dict): Keys are bank names, and values are dictionaries with 'date' and 'compra' information.
            - `venta_dict` (dict): Keys are bank names, and values are dictionaries with 'date' and 'venta' information.
            - `otro_dict` (dict): Keys are bank names, and values are dictionaries with 'date' and 'otro' information.
    """
    
    # Dictionaries to hold the separated data
    compra_dict = {}  # Dictionary to store entries with 'compra' information
    venta_dict = {}   # Dictionary to store entries with 'venta' information
    otro_dict = {}    # Dictionary to store entries with 'otro' information
    
    # Iterate over the list of dictionaries (entries)
    for entry in data:
        banco = entry['banco']   # Extract the bank name from the entry
        date = entry['date']     # Extract the date from the entry
        
        # Check if 'compra' key is present in the entry
        if 'compra' in entry:
            # Add to compra_dict with bank name as key
            # The value is a dictionary containing 'date' and 'compra' value
            compra_dict[banco] = {'date': date, 'compra': entry['compra']}
        
        # Check if 'venta' key is present in the entry
        if 'venta' in entry:
            # Add to venta_dict with bank name as key
            # The value is a dictionary containing 'date' and 'venta' value
            venta_dict[banco] = {'date': date, 'venta': entry['venta']}
        
        # Check if 'otro' key is present in the entry
        if 'otro' in entry:
            # Add to otro_dict with bank name as key
            # The value is a dictionary containing 'date' and 'otro' value
            otro_dict[banco] = {'date': date, 'otro': entry['otro']}
    
    # Return all three dictionaries
    return compra_dict, venta_dict, otro_dict

def dict_dataframe(data, status):
    """
    Converts a dictionary of bank data into a DataFrame for a specific status ('compra', 'venta', or 'otro').

    The function assumes that the input dictionary contains bank data with dates and various statuses. It creates a DataFrame
    where the index is the unique date and the columns are bank names with their respective values for the specified status.

    Args:
        data (dict): A dictionary where each key is a bank name and each value is another dictionary containing:
                     - 'date': A datetime object representing the date.
                     - Status keys ('compra', 'venta', 'otro'): Each holding corresponding data values.
        status (str): The key in the inner dictionaries whose values should be included in the DataFrame (e.g., 'compra', 'venta', 'otro').

    Returns:
        pd.DataFrame: A DataFrame with the date as the index and bank names as columns, containing values for the specified status.
    """
    
    # Extract unique dates from the data
    dates = set(item['date'] for item in data.values())  # Extract unique dates from the 'date' field
    date = list(dates)[0]  # Assuming there is only one unique date; take the first one from the list
    
    # Create a dictionary to hold the DataFrame data
    data_for_df = {}
    for bank, details in data.items():
        # Add each bank's status value to the dictionary
        data_for_df[bank] = details[status]
    
    # Create the DataFrame using the constructed dictionary and set the index to the unique date
    df = pd.DataFrame(data_for_df, index=[date])
    
    # Set 'date' as the index name (removing the default name)
    df.index.name = None
    
    return df

def Unify_Dataframe(data):
    """
    Processes a list of dictionaries to create three DataFrames for 'compra', 'venta', and 'otro' statuses.

    The function assumes each dictionary in the `data` list is a set of bank data entries. It utilizes helper functions `split_dict` and `dict_dataframe` to separate and organize data by status ('compra', 'venta', 'otro'). The results are concatenated into three distinct DataFrames.

    Args:
        data (list of dict): A list of dictionaries, where each dictionary contains bank data with a 'date' and potentially 'compra', 'venta', and 'otro' keys.

    Returns:
        tuple: A tuple containing three DataFrames:
            - `dfc` (pd.DataFrame): DataFrame with 'compra' values, indexed by date.
            - `dfv` (pd.DataFrame): DataFrame with 'venta' values, indexed by date.
            - `dfo` (pd.DataFrame): DataFrame with 'otro' values, indexed by date.
    """
    
    # Lists to collect DataFrames for each status
    list_compra = []  # List to hold DataFrames with 'compra' data
    list_venta = []   # List to hold DataFrames with 'venta' data
    list_otro = []    # List to hold DataFrames with 'otro' data
    
    # Iterate over the list of data dictionaries
    for x in data:
        # Split the dictionary into separate dictionaries for 'compra', 'venta', and 'otro'
        compra, venta, otro = split_dict(x)
        
        # Convert each dictionary to a DataFrame for the specified status
        compra = dict_dataframe(compra, 'compra')
        venta = dict_dataframe(venta, 'venta')
        otro = dict_dataframe(otro, 'otro')
    
        # Append the resulting DataFrames to the respective lists
        list_compra.append(compra) 
        list_venta.append(venta) 
        list_otro.append(otro) 
    
    # Concatenate all DataFrames in each list into a single DataFrame
    dfc = pd.concat(list_compra)  # Combine all 'compra' DataFrames
    dfv = pd.concat(list_venta)   # Combine all 'venta' DataFrames
    dfo = pd.concat(list_otro)    # Combine all 'otro' DataFrames

    return dfc, dfv, dfo

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

dfc, dfv, dfo = Unify_Dataframe(data)