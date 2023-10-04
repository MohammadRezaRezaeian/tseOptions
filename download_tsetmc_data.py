import asyncio
import aiohttp
from date_conversion import generate_jalali_date_range
from jdatetime import date, timedelta
import os
import time
import concurrent.futures
import pandas as pd


start_date = '1401-01-01'
end_date = date.today().isoformat()
# Download the datebase

# start_date = (date.today() - timedelta(days=30)).isoformat()  # type: ignore
# end_date = date.today().isoformat()
download_dates = generate_jalali_date_range(start_date, end_date)


# Download a single trading day
async def download_and_save_tsetmc(jalali_date: str, output_directory="./tsetmcdata"):
    # Define the base URL
    base_url = "https://members.tsetmc.com/tsev2/excel/MarketWatchPlus.aspx?d="

    # Concatenate the base URL with the Jalali date
    url = f"{base_url}{jalali_date}"
    # jalali_date = jalali_date.replace("-", "")

    # Define the output file path
    filename = os.path.join(output_directory, f"{jalali_date}.xlsx")

    # Check if the file already exists
    if os.path.exists(filename):
        print(f"File {jalali_date} already exists. Skipping download.")
        return

    async with aiohttp.ClientSession() as session:
        try:
            # Send an asynchronous HTTP GET request to the URL
            async with session.get(url) as response:
                # Check if the request was successful (status code 200)
                if response.status == 200:
                    # Check if the file size is greater than 5 KB (Check for trading days)
                    content = await response.read()
                    if len(content) / 1024 > 5:
                        # Save the content to the specified file
                        with open(filename, "wb") as f:
                            f.write(content)
                        print(
                            f"CSV data for {jalali_date} saved to {filename}")
                    else:
                        print(f"{jalali_date} is not a trading day.")
                else:
                    print(
                        f"Failed to download CSV for {jalali_date}. Status code:", response.status)
        except Exception as e:
            print(f"Error while downloading CSV for {jalali_date}:", str(e))


# Asynchronously download the data
# Create an event loop and run asynchronous downloads


tic = time.time()
async def main():
    tasks = [download_and_save_tsetmc(date)
             for date in download_dates]
    await asyncio.gather(*tasks)

# ------------------------------ generating the download sequence dates--------------#
# (date.today() - timedelta(days=180)).isoformat()  # type: ignore
download_dates = generate_jalali_date_range(start_date, end_date)

tic = time.time()  # For measuring the runtime
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

toc = time.time()  # For measuring the runtime

print(toc-tic)
