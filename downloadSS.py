from configparser import ConfigParser
from pathlib import Path

import boto3
from botocore import UNSIGNED
from botocore.config import Config


def download_SS(version="last", dest_dir="SemanticScholar"):
    """
    Download SemanticScholar info into `dest_dir`/`version`
    """

    # Create client to download from S3
    client = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    bucket = boto3.resource("s3", config=Config(signature_version=UNSIGNED)).Bucket(
        "ai2-s2-research-public"
    )
    # Get all releases
    prefix = "open-corpus/"
    releases = client.list_objects(Bucket=bucket.name, Prefix=prefix, Delimiter="/")
    releases = releases.get("CommonPrefixes")
    releases = sorted([o["Prefix"].split("/")[1] for o in releases])

    if version == "last":
        version = releases[-1]
    if version not in releases:
        print("Invalid version")
        return

    # Create dir to save files
    version_dir = version.replace("-", "")
    version_dir = Path(dest_dir).joinpath(version_dir)
    version_dir.mkdir(parents=True, exist_ok=True)

    count = 0
    print("Downloading...")
    for obj in bucket.objects.filter(Prefix=prefix + version):
        file_name = obj.key.split("/")[-1]
        if file_name.endswith(".gz"):
            print(file_name)
            bucket.download_file(
                obj.key, version_dir.joinpath(file_name).as_posix()
            )  # save to same path
        count += 1
        if count > 10:
            break


if __name__ == "__main__":
    cf = ConfigParser()
    cf.read("config.cf")
    dest_dir = cf.get("data", "dir_data")
    version = cf.get("data", "version")

    download_SS(version=version, dest_dir=dest_dir)
