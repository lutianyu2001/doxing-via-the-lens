import pandas as pd
import re
import argparse
import googlemaps
import os
from tqdm import tqdm

def parse_dms(dms_str):
    """
    Parse DMS (Degrees, Minutes, Seconds) coordinates and convert to decimal degrees.
    
    Example format: "37 deg 36' 55.54\" N".
    """
    if pd.isna(dms_str) or not isinstance(dms_str, str):
        return None
        
    pattern = r"""^\s*
        (\d+)\s*deg\s+          # Degrees
        (\d+)\s*['′]\s+         # Minutes (supporting both ' and ′)
        (\d+(?:\.\d+)?)\s*["″]\s+  # Seconds (supporting both " and ″)
        ([NSEW])\s*$            # Direction
    """
    match = re.match(pattern, dms_str.strip(), re.VERBOSE | re.IGNORECASE)
    if not match:
        raise ValueError(f"Invalid DMS format: {dms_str}")
    
    degrees, minutes, seconds, direction = match.groups()
    degrees = int(degrees)
    minutes = int(minutes)
    seconds = float(seconds)

    # Convert to decimal degrees
    decimal = degrees + minutes / 60 + seconds / 3600

    # Adjust sign for South and West
    if direction.upper() in ['S', 'W']:
        decimal *= -1

    return decimal

def get_address_from_coordinates(gmaps_client, lat, lng):
    """
    Get address from coordinates using Google Maps API.
    """
    if pd.isna(lat) or pd.isna(lng):
        return "No coordinates available"
        
    try:
        result = gmaps_client.reverse_geocode((lat, lng))
        if result:
            return result[0]['formatted_address']
        return "Address not found"
    except Exception as e:
        return f"Error getting address: {str(e)}"

def process_row(row, gmaps_client):
    """Process a single row of data to extract coordinates and get address."""
    try:
        lat_dms = row.get('GPSLatitude', '')
        lng_dms = row.get('GPSLongitude', '')
        
        if pd.isna(lat_dms) or pd.isna(lng_dms) or not lat_dms or not lng_dms:
            return pd.Series({
                'address': "No coordinates available",
                'latitude': None,
                'longitude': None
            })
        
        lat = parse_dms(lat_dms)
        lng = parse_dms(lng_dms)
        
        address = get_address_from_coordinates(gmaps_client, lat, lng)
        
        return pd.Series({
            'address': address,
            'latitude': lat,
            'longitude': lng
        })
    except Exception as e:
        return pd.Series({
            'address': f"Error: {str(e)}",
            'latitude': None,
            'longitude': None
        })

def process_coordinates_file(input_file, output_file, api_key, show_progress=True):
    """
    Process a CSV file containing GPS coordinates and generate addresses.
    
    Args:
        input_file (str): Path to the input CSV file with GPS coordinates
        output_file (str): Path where the output CSV file will be saved
        api_key (str): Google Maps API key
        show_progress (bool): Whether to show a progress bar during processing
    
    Returns:
        pandas.DataFrame: The processed DataFrame with addresses
    
    Raises:
        FileNotFoundError: If the input file does not exist
    """
    # Check if input file exists
    if not os.path.isfile(input_file):
        raise FileNotFoundError(f"Input file '{input_file}' not found")
    
    # Initialize Google Maps client
    gmaps = googlemaps.Client(key=api_key)
    
    # Read CSV file
    df = pd.read_csv(input_file)
    
    # Create a tqdm progress bar for the apply operation if requested
    if show_progress:
        tqdm.pandas(desc="Processing coordinates")
        apply_method = df.progress_apply
    else:
        apply_method = df.apply
    
    # Prepare result dataframe
    result_df = df[['SourceFile']].copy()
    result_df = result_df.rename(columns={'SourceFile': 'filename'})
    
    # Apply the processing function
    processed_results = apply_method(lambda row: process_row(row, gmaps), axis=1)
    
    # Combine the filename column with the processed results
    result_df = pd.concat([result_df, processed_results], axis=1)
    
    # Save results if output_file is provided
    if output_file:
        result_df.to_csv(output_file, index=False)
        print(f"Results saved to {output_file}")
    
    return result_df

def main():
    parser = argparse.ArgumentParser(description='Process EXIF GPS data and get addresses.')
    parser.add_argument('--input', '-i', type=str, required=True, help='Input CSV file path')
    parser.add_argument('--output', '-o', type=str, required=True, help='Output CSV file path')
    parser.add_argument('--api-key', '-k', type=str, required=True, help='Google Maps API key')
    
    args = parser.parse_args()
    
    process_coordinates_file(args.input, args.output, args.api_key)
    return 0

if __name__ == "__main__":
    exit(main())
