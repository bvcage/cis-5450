# CIS-5450

## Installation

1. Clone this repository and navigate to the root folder.

2. Setup & activate your venv
    ```
    $ python -m venv .venv
    $ source .venv/bin/activate
    ```

3. Install dependencies
    ```
    pip install -r requirements.txt
    ```

4. Get API credentials from Ravelry: https://www.ravelry.com/api

5. Create a `.env` file in the root directory
    ```
    RAVELRY_USER = ""
    RAVELRY_PASSWORD = ""
    ```

6. (Optional) Repo owner may provide a Google Drive link with scraped data. Add this to your `.env` file.
    ```
    SCRAPED_DATA_URL = ""
    ```

7. Refer to notebooks for remaining setup & additional information.
