# Download the ZIP files from https://srtm.csi.cgiar.org/wp-content/uploads/files/srtm_5x5/TIFF/
# Extract the TIF files from the ZIP files, and delete the ZIP files
# TIF files saved in the same folder as this script

import os
import socket
import urllib
import urllib.request
import zipfile

MIN_X = 1
MAX_X = 72
MIN_Y = 1
MAX_Y = 25

def read_url(url: str):
    buffer = None
    while buffer is None:
        try:
            buffer = urllib.request.urlopen(url, timeout=5)
        except socket.timeout:
            print("* Connection timeout detected")
    return buffer.read()


def download_zip(filename):
    url = "https://srtm.csi.cgiar.org/wp-content/uploads/files/srtm_5x5/TIFF/" + filename
    
    try:
        content = read_url(url)
        file = open(filename, "wb")
        file.write(content)
        file.close()
        print(url, "downloaded")
        return True
    except urllib.error.HTTPError as e:
        if e.code == 404:
            print(url, "does not exists")
            return False
        else:
            raise


def extract_tif(filebase):
    with zipfile.ZipFile(filebase + ".zip", "r") as zip_file:
        # zip_file.extract(filebase + ".hdr")
        zip_file.extract(filebase + ".tif")
        # zip_file.extract(filebase + ".tfw")


if __name__ == "__main__":
    for i in range(MIN_X, MAX_X + 1):
        for j in range(MIN_Y, MAX_Y + 1):
            filebase = "srtm_%s_%s" %(str(i).zfill(2), str(j).zfill(2))
            file_exists = download_zip(filebase + ".zip")
            
            if file_exists:
                # Get TIF from zip
                extract_tif(filebase)
                os.remove(filebase + ".zip")

                # Translate TIF to BIL
                # os.popen("gdal_translate -of ENVI {0}.tif {0}.bil".format(filebase))
                # os.popen("./srtm2df *.bil")