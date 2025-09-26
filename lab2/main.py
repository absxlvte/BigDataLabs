import pandas as pd
import matplotlib.pyplot as plt
import time
import os
import glob
def loadData(path: str) -> pd.DataFrame:
    try:
        data = pd.read_excel(path)
        #data = pd.read_csv(path)
        return data
    except FileNotFoundError:
        print(f"Ошибка! файл {path} не найден!")
        raise
def plotGraphicsBasicaly(data: pd.DataFrame) -> None:
    fig, axes = plt.subplots(nrows=1,ncols=3, figsize=(18,5))
    time_start = time.time()
    breach_counts = data['Type of Breach'].value_counts()
    axes[0].bar(breach_counts.index,breach_counts.values)
    axes[0].grid(axis='y',alpha=0.3)
    time_finish = time.time() - time_start
    axes[0].set_title(f'Типы утечек -> {time_finish:.4f} с',fontsize=14, fontweight='bold')
    axes[0].tick_params(axis='x', rotation=90)
    time_start = time.time()
    entity_counts = data['Covered Entity Type'].value_counts()
    patches,text,autotext = axes[1].pie(entity_counts.values,labels=entity_counts.index,autopct='%1.1f%%')
    time_finish = time.time() - time_start
    axes[1].set_title(f'Типы организаций -> {time_finish:.4f} с',fontsize=14, fontweight='bold')
    time_start = time.time()
    if data['Breach Submission Date'].dtype == 'object':
        data['Breach Submission Date'] = pd.to_datetime(data['Breach Submission Date'])
    timeline = data.groupby(data['Breach Submission Date'].dt.to_period('M')).size()
    timeline = timeline.reset_index()
    timeline['Breach Submission Date'] = timeline['Breach Submission Date'].dt.to_timestamp()
    time_finish = time.time() - time_start
    axes[2].plot(timeline['Breach Submission Date'], timeline[0],marker='o', linewidth=2, markersize=4, color='red')
    axes[2].set_title(f'Утечки от времени -> {time_finish:.4f} c',fontsize=14, fontweight='bold')
    axes[2].set_xlabel('Дата', fontsize=12)
    axes[2].set_ylabel('Количество утечек в месяц', fontsize=12)
    axes[2].tick_params(axis='x', rotation=45)
    axes[2].grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()

def dataSectionalizationType(data: pd.DataFrame, output_path: str='./Sectionalizationed/type') -> None:
    os.makedirs(output_path,exist_ok=True)
    partitions = {}
    cnt = 0
    for entity_type, entity_data in data.groupby('Covered Entity Type'):
        safe_name = entity_type.replace(' ','_').replace('/', '_')
        partition_name = f'entity_{safe_name}'
        partitions[partition_name] = entity_data
        filename = f'{output_path}/{partition_name}.csv'
        entity_data.to_csv(filename,index=False)
        print(f'создано: {partition_name} : {len(entity_data)} записей')
        cnt += 1
    print(f'создано {cnt} шт')

def plotGraphicsAdvanced(full_data: pd.DataFrame, target_entity: str = 'Healthcare Provider', partition_path: str = './Sectionalizationed/type') -> None:
    partitions = loadSectionalizedData(partition_path)
    if not partitions:
        print("Секционированные данные не найдены. Создаем заново...")
        dataSectionalizationType(full_data, partition_path)
        partitions = loadSectionalizedData(partition_path)
    fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(18, 5))
    safe_name = target_entity.replace(' ','_')
    currentData = partitions[f'entity_{safe_name}']
    time_start = time.time()
    if currentData['Breach Submission Date'].dtype == 'object':
        currentData['Breach Submission Date'] = pd.to_datetime(currentData['Breach Submission Date'])
    timeline = currentData.groupby(currentData['Breach Submission Date'].dt.to_period('M')).size()
    timeline = timeline.reset_index()
    timeline['Breach Submission Date'] = timeline['Breach Submission Date'].dt.to_timestamp()
    axes[0][0].plot(timeline['Breach Submission Date'], timeline[0], marker='o', linewidth=2, markersize=4, color='red')
    axes[0][0].set_xlabel('Дата', fontsize=12)
    axes[0][0].set_ylabel('Количество утечек в месяц', fontsize=12)
    axes[0][0].tick_params(axis='x', rotation=45)
    axes[0][0].grid(True, alpha=0.3)
    time_finish = time.time() - time_start
    axes[0][0].set_title(f'Утечки от времени (sect)-> {time_finish:.5f} c', fontsize=14, fontweight='bold')
    currentData = full_data
    time_start = time.time()
    currentData = currentData[currentData['Covered Entity Type'] == target_entity]
    if currentData['Breach Submission Date'].dtype == 'object':
        currentData['Breach Submission Date'] = pd.to_datetime(currentData['Breach Submission Date'])
    timeline = currentData.groupby(currentData['Breach Submission Date'].dt.to_period('M')).size()
    timeline = timeline.reset_index()
    timeline['Breach Submission Date'] = timeline['Breach Submission Date'].dt.to_timestamp()
    axes[1][0].plot(timeline['Breach Submission Date'], timeline[0], marker='o', linewidth=2, markersize=4, color='red')
    axes[1][0].set_xlabel('Дата', fontsize=12)
    axes[1][0].set_ylabel('Количество утечек в месяц', fontsize=12)
    axes[1][0].tick_params(axis='x', rotation=45)
    axes[1][0].grid(True, alpha=0.3)
    time_finish = time.time() - time_start
    axes[1][0].set_title(f'Утечки от времени (full base)-> {time_finish:.5f} c', fontsize=14, fontweight='bold')
    currentData = partitions[f'entity_{safe_name}']
    time_start = time.time()
    breach_counts = currentData['Type of Breach'].value_counts()
    axes[0][1].bar(breach_counts.index, breach_counts.values)
    axes[0][1].grid(axis='y', alpha=0.3)
    time_finish = time.time() - time_start
    axes[0][1].set_title(f'Типы утечек (sect) -> {time_finish:.5f} с', fontsize=14, fontweight='bold')
    axes[0][1].tick_params(axis='x', rotation=90)
    currentData = full_data
    time_start = time.time()
    currentData = currentData[currentData['Covered Entity Type'] == target_entity]
    breach_counts = currentData['Type of Breach'].value_counts()
    axes[1][1].bar(breach_counts.index, breach_counts.values)
    axes[1][1].grid(axis='y', alpha=0.3)
    axes[1][1].tick_params(axis='x', rotation=90)
    time_finish = time.time() - time_start
    axes[1][1].set_title(f'Типы утечек (full base) -> {time_finish:.5f} с', fontsize=14, fontweight='bold')


    plt.show()


def loadSectionalizedData(path: str = './Sectionalizationed/type') -> dict:
    partitions = {}
    if not os.path.exists(path):
        print(f"Ошибка: Папка {path} не существует!")
        return partitions
    csv_files = glob.glob(os.path.join(path, "*.csv"))
    if not csv_files:
        print(f"В папке {path} не найдено CSV файлов")
        return partitions
    for file_path in csv_files:
        try:
            filename = os.path.basename(file_path)
            partition_name = filename.replace('.csv', '')
            partition_data = pd.read_csv(file_path)
            partitions[partition_name] = partition_data
            print(f"Загружено: {partition_name} - {len(partition_data)} записей")
        except Exception as e:
            print(f"Ошибка загрузки файла {file_path}: {e}")
    print(f"\nУспешно загружено секций: {len(partitions)}")
    return partitions


if __name__ == '__main__':
    Data = loadData("Утечки мед данных.xls")
    plotGraphicsBasicaly(Data)
    #dataSectionalizationType(Data)
    #plotGraphicsAdvanced(Data)