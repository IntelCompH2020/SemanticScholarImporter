from configparser import ConfigParser
from pathlib import Path
import requests
import urllib
import json

# This script is based on the s2ag.py provided with the sample dataset by S2AI

def download_S2(token, dest_dir, version="latest"):
    """
    Download SemanticScholar info into `dest_dir`/`version`
    """

    # Download information about latest version
    data = requests.get('https://api.semanticscholar.org/datasets/v1/release/' + version + '/')

    if data.ok:

        jsonData = data.json()

        release = jsonData['release_id']
        datasets = [d['name'] for d in jsonData['datasets']]

        dest_dir = dest_dir.joinpath(release.replace('-','')).joinpath('rawdata')
        dest_dir.mkdir(parents=True, exist_ok=True)
        
        with dest_dir.joinpath('release_info.json').open('w') as jsonFile:
            json.dump(jsonData, jsonFile, indent=4)

        # Start downloading all datasets
        # for dtset in datasets:
        # for dtset in ['tldrs', 'abstracts', 'authors', 'paper-ids', 'papers', 'publication-venues', 'citations']:
        for dtset in ['s2orc', 'embeddings']:
            url = "http://api.semanticscholar.org/datasets/v1/release/" + release + "/dataset/"+dtset
            dtset_info = requests.get(url, headers={'x-api-key':token})
            
            if dtset_info.ok:
                print("\nDownloading dataset " + dtset)
                dtsetData = dtset_info.json()

                dtset_dir = dest_dir.joinpath(dtset)
                dtset_dir.mkdir(parents=False, exist_ok=True)

                with dtset_dir.joinpath(dtset+'info.json').open('w') as jsonFile:
                    json.dump(dtsetData, jsonFile, indent=4)

                for idx, fileUrl in enumerate(dtsetData['files']):
                    print("Downloading file", idx, "of", len(dtsetData['files']))
                    fileName = dtset_dir.joinpath(dtset + "-part" + str(idx) + ".json.gz")
                    if not fileName.is_file():
                        urllib.request.urlretrieve(fileUrl, fileName)

    return


if __name__ == "__main__":
    cf = ConfigParser()
    cf.read("config.cf")
    dest_dir = Path(cf.get("downloadS2", "dir_data"))
    version = cf.get("downloadS2", "version")
    token = cf.get("downloadS2", "S2_API_KEY")

    download_S2(token=token, dest_dir=dest_dir, version=version)
