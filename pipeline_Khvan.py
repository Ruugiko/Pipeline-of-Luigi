import luigi 
import os
import pandas as pd
import tarfile
import wget 
from bs4 import BeautifulSoup
import requests
import io
import gzip

class DownloadDataset(luigi.Task):
    dataset_name = luigi.Parameter()

    def run(self):
        # Создаем директорию, если она еще не существует
        dataset_dir = os.path.join('data', self.dataset_name)
        os.makedirs(dataset_dir, exist_ok=True)

        # Получаем страницу
        geo_url = f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={self.dataset_name}"
        response = requests.get(geo_url)
        response.raise_for_status()  # Отслеживаем ошибку

        # Парсим страницу для поиска ссылки для скачивания
        soup = BeautifulSoup(response.text, 'html.parser')
        supplementary_section = soup.find(text="Supplementary file")
        if supplementary_section:
            sup_link = supplementary_section.find_next('a', href=True)
            if sup_link and 'download' in sup_link['href']:
                download_url = f"https://www.ncbi.nlm.nih.gov{sup_link['href']}"

                # Скачиваем файлы
                wget.download(download_url, os.path.join(dataset_dir, f"{self.dataset_name}_RAW.tar"))

    def output(self):
        return luigi.LocalTarget(os.path.join('data', self.dataset_name, f"{self.dataset_name}_RAW.tar"))
    
class ExtractTarGz(luigi.Task):
    dataset_name = luigi.Parameter()

    def requires(self):
        return DownloadDataset(self.dataset_name)

    def run(self):
        # Извлекаем содержимое tar-архива
        tar = tarfile.open(self.input().path)
        tar.extractall(path=self.dataset_name)
        tar.close()

        # Получаем список файлов в директории
        file_list = os.listdir(self.dataset_name)
        for file_name in file_list:
            file_path = os.path.join(self.dataset_name, file_name)
            
            # Если файл является gzip-архивом, извлекаем его содержимое
            if file_name.endswith('.gz'):
                with gzip.open(file_path, 'rb') as f_in:
                    with open(file_path[:-3], 'wb') as f_out:  # удаляем '.gz' из имени файла
                        f_out.write(f_in.read())
                os.remove(file_path)  # Удаляем gzip-архив после извлечения

        # Обновляем список файлов после извлечения gzip-файлов
        file_list = [file for file in os.listdir(self.dataset_name) if not file.endswith('.gz')]
        
        # Записываем список файлов в выходной файл
        with self.output().open('w') as f:
            for file_name in file_list:
                f.write(file_name + '\n')

    def output(self):
        return luigi.LocalTarget(f"{self.dataset_name}/file_list.txt")


class ProcessFiles(luigi.Task):
    dataset_name = luigi.Parameter()

    def requires(self):
        return ExtractTarGz(self.dataset_name)

    def output(self):
        return luigi.LocalTarget(f"{self.dataset_name}")

    def run(self):
        file_list_path = self.input().path
        with self.input().open('r') as file_list:
            for file_name in file_list:
                stripped_file_name = file_name.strip()
                file_path = os.path.join(self.dataset_name, stripped_file_name)
                # Создать папку с именем файла
                file_dir = os.path.splitext(stripped_file_name)[0]
                folder_path = os.path.join(self.dataset_name, file_dir)
                os.makedirs(folder_path, exist_ok=True)
                # Обработка файла
                dfs = {}
                with open(file_path) as f:
                    write_key = None
                    fio = io.StringIO()
                    for line in f.readlines():
                        if line.startswith('['):
                            if write_key:
                                fio.seek(0)
                                header = None if write_key == 'Heading' else 'infer'
                                dfs[write_key] = pd.read_csv(fio, sep='\t', header=header)
                            fio = io.StringIO()
                            write_key = line.strip('[]\n')
                            continue
                        if write_key:
                            fio.write(line)
                    if write_key:
                        fio.seek(0)
                        dfs[write_key] = pd.read_csv(fio, sep='\t')
                # Сохранение таблиц в созданную папку
                for k, v in dfs.items():
                    table_file_name = os.path.join(folder_path, k + '.tsv')
                    v.to_csv(table_file_name, sep='\t', index=False)
                os.remove(file_path)
                
                # Process Probes table
                probes_file_path = os.path.join(folder_path, 'Probes.tsv')
            if os.path.exists(probes_file_path):
                probes_df = pd.read_csv(probes_file_path, sep='\t')
                probes_df.drop(columns=[
                    'Definition',
                    'Ontology_Component',
                    'Ontology_Process',
                    'Ontology_Function',
                    'Synonyms',
                    'Obsolete_Probe_Id',
                    'Probe_Sequence'
                ], inplace=True)
                probes_df.to_csv(probes_file_path, sep='\t', index=False)
            
            os.remove(file_list_path)  # Удаляем файл со списком файлов

class Pipeline(luigi.WrapperTask):
    dataset_name = luigi.Parameter(default='GSE68849')

    def requires(self):
        return ProcessFiles(self.dataset_name)
    
    
if __name__ == '__main__':
    luigi.run()    