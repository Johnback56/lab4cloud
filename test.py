import json
from pathlib import Path

import boto3
import pandas as pd
import requests
import matplotlib.pyplot as plt
from botocore.exceptions import ClientError

REGION = "eu-north-1"          
BUCKET_NAME = "bucket123"
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

USD_JSON = DATA_DIR / "usd_2022.json"
EUR_JSON = DATA_DIR / "eur_2022.json"
CSV_FILE = DATA_DIR / "currency_2022.csv"
PLOT_FILE = DATA_DIR / "currency_2022.png"


def fetch_currency_range(valcode: str, start: str, end: str, output_file: Path) -> list:
    url = (
        f"https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange"
        f"?valcode={valcode}&start={start}&end={end}&json"
    )

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    data = response.json()

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"JSON saved: {output_file}")
    return data


def convert_to_csv(usd_data: list, eur_data: list, output_csv: Path) -> pd.DataFrame:
    usd_df = pd.DataFrame(usd_data)[["exchangedate", "rate"]].copy()
    eur_df = pd.DataFrame(eur_data)[["exchangedate", "rate"]].copy()

    usd_df.columns = ["date", "usd_rate"]
    eur_df.columns = ["date", "eur_rate"]

    df = pd.merge(usd_df, eur_df, on="date", how="inner")
    df["date"] = pd.to_datetime(df["date"], format="%d.%m.%Y")
    df = df.sort_values("date")

    df.to_csv(output_csv, index=False, encoding="utf-8")
    print(f"CSV saved: {output_csv}")
    return df


def create_bucket_if_not_exists(bucket_name: str, region: str) -> None:
    s3 = boto3.client("s3", region_name=region)

    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket already exists: {bucket_name}")
    except ClientError:
        if region == "us-east-1":
            s3.create_bucket(Bucket=bucket_name)
        else:
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region}
            )
        print(f"Bucket created: {bucket_name}")


def upload_file_to_s3(local_file: Path, bucket_name: str, s3_key: str) -> None:
    s3 = boto3.client("s3", region_name=REGION)
    s3.upload_file(str(local_file), bucket_name, s3_key)
    print(f"Uploaded to S3: {s3_key}")


def read_csv_from_s3(bucket_name: str, s3_key: str) -> pd.DataFrame:
    s3 = boto3.client("s3", region_name=REGION)
    obj = s3.get_object(Bucket=bucket_name, Key=s3_key)
    df = pd.read_csv(obj["Body"])
    df["date"] = pd.to_datetime(df["date"])
    print("CSV read from S3")
    return df


def build_plot(df: pd.DataFrame, output_file: Path) -> None:
    plt.figure(figsize=(12, 6))
    plt.plot(df["date"], df["usd_rate"], label="USD/UAH")
    plt.plot(df["date"], df["eur_rate"], label="EUR/UAH")
    plt.title("Курс гривні до USD та EUR за 2022 рік")
    plt.xlabel("Дата")
    plt.ylabel("Курс")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_file)
    plt.close()
    print(f"Plot saved: {output_file}")


def main():
    start = "20220101"
    end = "20221231"

    usd_data = fetch_currency_range("USD", start, end, USD_JSON)
    eur_data = fetch_currency_range("EUR", start, end, EUR_JSON)

    df = convert_to_csv(usd_data, eur_data, CSV_FILE)

    create_bucket_if_not_exists(BUCKET_NAME, REGION)

    upload_file_to_s3(CSV_FILE, BUCKET_NAME, "currency_2022.csv")

    s3_df = read_csv_from_s3(BUCKET_NAME, "currency_2022.csv")

    build_plot(s3_df, PLOT_FILE)

    upload_file_to_s3(PLOT_FILE, BUCKET_NAME, "currency_2022.png")

    print("Done")


if __name__ == "__main__":
    main()
