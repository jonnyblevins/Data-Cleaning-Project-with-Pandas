#Create a data pipeline that automates the ingestion, cleaning, and preparation of data from a source for data analysis. Support/convert various data formats and sources. Identify and rectify outliers. Ensure it standardizes data types. Future proof, test and validate the code and yield a clean dataset.

import pandas as pd

def load_data(source, format='csv'):
    # Try block used for error handling
    try:
        if format == 'csv':
            df = pd.read_csv(source)
        elif format == 'parquet':
            df = pd.read_parquet(source)
        elif format == 'sql':
            df = pd.read_sql(source)
        #Add elif statements here for more data extensions
        else:
            raise ValueError("Unsupported format")

        # Structure validation for data validation
        expected_columns = ['Date', 'Transaction Amount', 'Name', 'Email', 'Payment Method', 'Address', 'Product Name']
        if not all(column in df.columns for column in expected_columns):
            raise ValueError("Missing one or more expected columns!")
        
        return df

    except Exception as e:
        print(f"Error loading data: {e}")
        raise

    
def report_missing_values(df):
    missing_values = df.isnull().sum()
    missing_percentage = (missing_values / len(df)) * 100
    missing_report = pd.DataFrame({'missing_values': missing_values, 'missing_percentage': missing_percentage})
    return missing_report[missing_report['missing_values'] > 0]

def clean_and_transform_data(df, cleaning_rules=None):
    # Make sure to report missing values before testing
    print("Missing Values Report:\n", report_missing_values(df))
    
    # Convert 'Date' to datetime (NOW I CAN USE THIS FOR ETL, haha)
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')

    # Handle non-numbers in 'Transaction Amount'
    df['Transaction Amount'] = pd.to_numeric(df['Transaction Amount'], errors='coerce')
    
    # DATA CLEANING TIME!
    def clean_name(name):
        return str(name).strip().title()
    import re
    def validate_email(email):
        if re.match(r"[^@]+@[^@]+\.[^@]+", email): #This one I had to ask for help with, as seen on "https://stackoverflow.com/questions/201323/how-can-i-validate-an-email-address-using-a-regular-expression , the full email check is (hahaha, surely there's an easier way I don't know about): (?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9]))\.){3}(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9])|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])"
            return email
        else:
            return None
    def standardize_date(date):
        return pd.to_datetime(date, errors='coerce')
    def standardize_payment_method(method):
        method = str(method).lower()
        if method in ['credit', 'credit card', 'cc']:
            return 'Credit Card'
        elif method in ['debit', 'debit card', 'dc']:
            return 'Debit Card'
        else:
            return method.title()
    def clean_address(address):
        return str(address).title().strip() 
    def standardize_product_name(name):
        return str(name).strip().title() 
    
    # Apply cleaning functions
    df = df.assign(
        Name=df['Name'].apply(clean_name),
        Email=df['Email'].apply(validate_email),
        Date=standardize_date(df['Date']),
        Payment_Method=df['Payment Method'].apply(standardize_payment_method),
        Address=df['Address'].apply(clean_address),
        Product_Name=df['Product Name'].apply(standardize_product_name)
    )

    df['Transaction Year'] = df['Date'].dt.year

    # Handle missing values
    df = df.fillna(method='ffill').fillna(method='bfill')
    #Output says this, but I'm running out of time to submit (I'll figure it out!): DataFrame.fillna with 'method' is deprecated and will raise in a future version.
    df['Normalized Transaction Amount'] = (df['Transaction Amount'] - df['Transaction Amount'].mean()) / df['Transaction Amount'].std()

    Q1 = df['Transaction Amount'].quantile(0.25)
    Q3 = df['Transaction Amount'].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    df = df[(df['Transaction Amount'] >= lower_bound) & (df['Transaction Amount'] <= upper_bound)]

    df = df.fillna(method='ffill').fillna(method='bfill')
    
    # Convert data types
    df = df.astype({
        'Name': 'string',
        'Email': 'string',
        'Date': 'datetime64[ns]',
        'Payment Method': 'category',
        'Address': 'string',
        'Product Name': 'category'
    })

    return df

def run_pipeline(data_source, format='csv', cleaning_rules=None):
    df = load_data(data_source, format)
    df = clean_and_transform_data(df, cleaning_rules)
    return df

#Replace this with the data source
data_source = 'https://raw.githubusercontent.com/BriDeWaltCCC/PFDADataSets/main/Project_1_Data_File.csv'
#C:/Users/Jonny/Desktop/Program/Python/Python II Spring 2024/4.1.2024/Project1Data.csv
cleaned_df = run_pipeline(data_source)
print(cleaned_df)