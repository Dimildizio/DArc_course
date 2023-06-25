import csv
import glob
import os
import zipfile


def unpack(filename, starts):
    with zipfile.ZipFile(filename, 'r') as zf:
        zf_name = next(f for f in zf.namelist() if f.startswith(starts) and f.endswith('.txt'))
        zf.extract(zf_name, path='result')
    return os.path.join(output_folder, zf_name)


def convert(filename, new_name=False):
    new_name = new_name if new_name else filename[:-3]+'csv'   # filename.replace('.txt', '.csv')
    with open(filename, 'r') as txt_f, open(new_name, 'w', newline='') as csv_f:
        writer = csv.writer(csv_f, delimiter=',')
        for line in txt_f:
            # not really required since excel and python don't take var types from .csv file
            # and pandas can specify data types for columns when using pd.read_csv
            val = [float(num) if '.' in num else int(num) for num in line.split()]
            writer.writerow(val)


def performer(name, name_start):
    unpacked_name = unpack(name, name_start)
    convert(unpacked_name)
    os.remove(unpacked_name)


if __name__ == '__main__':
    input_folder = 'data'
    output_folder = 'result'
    zip_files = glob.glob(f"{input_folder}/*.zip")
    for z in zip_files:
        performer(z, 'wr')
