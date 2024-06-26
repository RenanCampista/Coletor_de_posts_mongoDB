# Convert_extraction_in_csv

This script is used to convert the extraction of the data from the JSONs into a CSV file. The extraction is done using the `pandas` library.

## Usage

To use this script, you need to have the following libraries installed:
- pandas

```bash
pip install pandas
```

To run the script, you need to run the following command:

```bash
python script.py <path_to_json> <social_network>
```

Where:
- `<path_to_json>` is the path to the JSON file that contains the data to be extracted.
- `<social_network>` is the social network that the JSON file contains. It can be one of the following:
  - `twitter`
  - `instagram`
  - `facebook`
  - `tiktok`

The csv file will be saved in the same directory as the script.