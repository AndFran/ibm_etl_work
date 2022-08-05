import glob
import pandas as pd
from datetime import datetime
import xml.etree.ElementTree as ET

tmpfile = "dealership_temp.tmp"  # file used to store all extracted data
logfile = "dealership_logfile.txt"  # all event logs will be stored in this file
targetfile = "dealership_transformed_data.csv"  # file where transformed data is stored


def extract_csv(file_to_extract: str) -> pd.DataFrame:
    return pd.read_csv(file_to_extract)


def extract_json(file_to_extract: str) -> pd.DataFrame:
    return pd.read_json(file_to_extract, lines=True)


def extract_xml(file_to_extract: str) -> pd.DataFrame:
    dataframe = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])
    tree = ET.parse(file_to_extract)
    root = tree.getroot()
    for dealer in root:
        car_model = dealer.find("car_model").text
        year_of_manufacture = dealer.find("year_of_manufacture").text
        price = float(dealer.find("price").text)
        fuel = dealer.find("fuel").text

        dataframe = dataframe.append({"car_model": car_model,
                                      "year_of_manufacture": year_of_manufacture,
                                      "price": price,
                                      "fuel": fuel}, ignore_index=True)

    return dataframe


def extract() -> pd.DataFrame:
    result_data_frame = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])

    for csv_file in glob.glob("*.csv"):
        result_data_frame = result_data_frame.append(extract_csv(csv_file), ignore_index=True)

    for json_file in glob.glob("*.json"):
        result_data_frame = result_data_frame.append(extract_json(json_file), ignore_index=True)

    for xml_file in glob.glob("*.xml"):
        result_data_frame = result_data_frame.append(extract_xml(xml_file), ignore_index=True)

    return result_data_frame


def transform(data: pd.DataFrame) -> pd.DataFrame:
    data['price'] = round(data.price, 2)

    return data


def load(dataframe: pd.DataFrame):
    dataframe.to_csv(targetfile)


def log(message: str):
    timestamp_format = "%H:%M:%S-%h-%d-%Y"
    now = datetime.now()
    time_stamp = now.strftime(timestamp_format)
    with open(logfile, 'a') as f:
        f.write(f"{time_stamp} {message}\n")


log("Begin ETL process")

log("Begin extract")
result = extract()
log("End extract")

log("Begin transform")
result = transform(result)
log("End transform")

log("Begin load")
load(result)
log("End load")

log("End ETL Process")




