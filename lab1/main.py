import numpy as np
import matplotlib.pyplot as plt
import random
import pandas as pd
import time
import os
import keyboard
import math
from mimesis import Person, Address
from mimesis.locales import Locale
from mimesis.enums import Gender
from mimesis.builtins import RussiaSpecProvider


person = Person(locale=Locale.RU)
address = Address(locale=Locale.RU)
Russia_spec = RussiaSpecProvider()

def createData(NumRecords: int) -> list:
    Data = []
    academic_degree = ['Отсутствует','Бакалавр', 'Специалист', 'Магистр', 'Кандидат наук', 'Доктор наук']

    mean_work = 15
    std_work = 10
    min_work = 1
    max_work = 20

    mean_age = 30
    std_age = 15
    min_age = 18
    max_age = 65

    for _ in range(NumRecords):
        sex = person.gender()
        while True:
            work = int(random.gauss(mean_work, std_work))
            if min_work <= work <= max_work:
                break
        while True:
            age = int(random.gauss(mean_age, std_age))
            if min_age <= age <= max_age:
                break
        work_time = work
        if age < 25:
            weights = [0.6, 0.3, 0.1, 0.0, 0.0, 0.0]
        elif age < 40:
            weights = [0.3, 0.3, 0.2, 0.15, 0.05, 0.0]
        else:
            weights = [0.4, 0.2, 0.15, 0.1, 0.1, 0.05]
        degree = random.choices(academic_degree, weights, k=1)[0]
        salary = int(2000*math.exp(work_time*0.15)*math.exp(age*0.1) if age<60 else 20000*math.exp(work_time*0.15)*math.exp((age-60)*(-0.05)))
        people = [person.full_name(gender = Gender.MALE if sex=='Муж.' else Gender.FEMALE),
                  sex, age, degree, salary, address.city(),
                  str(Russia_spec.passport_series())+' '+str(Russia_spec.passport_number()),work_time]
        Data.append(people)
    return Data

def printData(Data: list, NumOutput: int = None) -> None:
    print(f'№|ФИО|Пол|Возраст|Академическая степень|Зарплата|Город|Серия номер паспорта|Трудовой стаж')
    for i, people in enumerate(Data):
        if NumOutput is not None and i>=NumOutput:
            break
        print(f'{i+1}.',*people, sep=' | ')


def visualizateData(Data: list) -> None:
    salaries = np.array([people[4] for people in Data])
    ages = np.array([people[2] for people in Data])
    work_times = np.array([people[7] for people in Data])
    cities = [people[5] for people in Data]
    genders = [people[1] for people in Data]
    degrees = [people[3] for people in Data]

    fig, axes = plt.subplots(3, 2, figsize=(16, 9))

    sorted_indices = np.argsort(ages)
    sorted_salaries = salaries[sorted_indices]
    sorted_ages = ages[sorted_indices]
    age = set()
    sal = {}
    for a in sorted_ages:
        if a not in age:
            age.add(a)
            sal.update({a:np.mean([sorted_salaries[i] for i in range(len(sorted_ages)) if sorted_ages[i] == a])})

    axes[0, 0].plot(sal.keys(), sal.values(), 'k', linewidth=2)
    axes[0, 0].set_title('Зависимость з/п от возраста', fontsize=12, fontweight='bold')
    axes[0, 0].set_xlabel('Возраст')
    axes[0, 0].set_ylabel('Средняя зарплата (руб.)')
    axes[0, 0].grid(True, alpha=0.3)

    sorted_indices = np.argsort(work_times)
    sorted_worktime = work_times[sorted_indices]
    sorted_salaries = salaries[sorted_indices]
    work = set()
    sal = {}
    for w in sorted_worktime:
        if w not in work:
            work.add(w)
            sal.update({w: np.mean([sorted_salaries[i] for i in range(len(sorted_worktime)) if sorted_worktime[i] == w])})

    axes[0, 1].plot(sal.keys(), sal.values(), 'k', linewidth=2)
    axes[0, 1].set_title('Зависимость з/п от трудового стажа', fontsize=12, fontweight='bold')
    axes[0, 1].set_xlabel('Стаж (лет)')
    axes[0, 1].set_ylabel('Средняя зарплата (руб.)')
    axes[0, 1].grid(True, alpha=0.3)

    city_series = pd.Series(cities)
    top_10_cities = city_series.value_counts().head(10)
    other_count = len(cities) - top_10_cities.sum()

    if other_count > 0:
        top_10_cities = pd.concat([top_10_cities, pd.Series({'Другие': other_count})])

    wedges = axes[1, 0].pie(top_10_cities.values,startangle=90,colors=plt.cm.Set3(np.linspace(0, 1, len(top_10_cities))))[0]
    axes[1, 0].set_title('Распределение по городам', fontsize=12, fontweight='bold')
    total = sum(top_10_cities.values)
    legend_labels = [f'{city} - {count} ({count / total * 100:.1f}%)' for city, count in top_10_cities.items()]
    axes[1, 0].legend(wedges, legend_labels, title="Города", loc="center left", bbox_to_anchor=(1, 0, 0.5, 1), fontsize=8)

    male_mask = np.array(genders) == 'Муж.'
    female_mask = np.array(genders) == 'Жен.'

    male_avg_salary = np.mean(salaries[male_mask]) if male_mask.any() else 0
    female_avg_salary = np.mean(salaries[female_mask]) if female_mask.any() else 0

    bars = axes[1, 1].bar(['Мужчины', 'Женщины'], [male_avg_salary, female_avg_salary],
                          alpha=0.7, color=['blue', 'pink'])
    axes[1, 1].set_title('Средняя зарплата по полу', fontsize=12, fontweight='bold')
    axes[1, 1].set_ylabel('Средняя зарплата (руб.)')

    for bar in bars:
        height = bar.get_height()
        axes[1, 1].text(bar.get_x() + bar.get_width() / 2., height + 1000,f'{int(height):,}'.replace(',', ' '),ha='center', va='bottom')

    unique_degrees, counts = np.unique(degrees, return_counts=True)
    axes[2, 0].pie(counts, labels=unique_degrees, autopct='%1.1f%%', startangle=90)
    axes[2, 0].set_title('Распределение ученых степеней', fontsize=12, fontweight='bold')

    scatter = axes[2, 1].scatter(ages, salaries, alpha=0.6, c=work_times, cmap='viridis')
    axes[2, 1].set_title('Диаграмма рассеивания: зарплата-возраст', fontsize=12, fontweight='bold')
    axes[2, 1].set_xlabel('Возраст (лет)')
    axes[2, 1].set_ylabel('Зарплата (руб.)')

    cbar = plt.colorbar(scatter, ax=axes[2, 1])
    cbar.set_label('Трудовой стаж (лет)')

    plt.tight_layout()
    plt.show()
def saveData(Data:list, filepath: str, filename:str='Data') -> None :
    with open(filepath+'\\'+filename+'.txt','w',encoding='utf-8-sig') as f:
        f.write(f'ФИО,Пол,Возраст,Академическая степень,Зарплата,Город,Серия номер паспорта,Трудовой стаж\n')
        for people in Data:
            f.write(','.join(map(str, people)) + '\n')

def speedTest() -> None:
    speed = {}
    for cnt in range(1,100001,5000):
        start_time = time.time()
        createData(cnt)
        elapsed_time = time.time() - start_time
        speed.update({cnt:elapsed_time})
    plt.figure()
    plt.plot(speed.keys(),speed.values(),'k')
    plt.title('Зависимость скорости генерации от числа записей', fontsize=14, fontweight='bold')
    plt.xlabel('Количество записей')
    plt.ylabel('Время выполнения (секунды)')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()

def menu() -> int:
    print('1.Сгенерировать записи\n2.Визуализировать записи\n3.Сохранить записи\n4.Тест быстродействия\n5.Выход')
    try:
        command = int(input('Введите номер команды '))
        if command not in range(1,6):
            os.system('cls')
            print('Введите команду верно')
            return None
    except ValueError:
        os.system('cls')
        print("Введите корректное число!")
        return None
    os.system('cls')
    match command:
        case 1:
            try:
                number = int(input('Введите число записей '))
                Data = createData(number)
                visible_number = int(input('Введите число отображаемых записей '))
                printData(Data,visible_number)
            except ValueError:
                os.system('cls')
                print("Введите корректное число!")
                return None
            print('Нажмите пробел для возврата к меню')
            while 1:
                if keyboard.is_pressed('space'):
                    os.system('cls')
                    break
        case 2:
            try:
                number = int(input('Введите число записей '))
                Data = createData(number)
                visualizateData(Data)
            except ValueError:
                os.system('cls')
                print("Введите корректное число!")
                return None
        case 3:
            try:
                number = int(input('Введите число записей '))
                filename = str(input('Введите название файла (без расширения)')).strip()
                filepath = str(input('Введите путь к директории')).strip()
                Data = createData(number)
                saveData(Data,filepath,filename)
            except:
                os.system('cls')
                print("Введите корректное число!")
                return None
        case 4:
            speedTest()
        case 5:
            return 1
if __name__ == '__main__':
    while(1):
        val = menu()
        if val:
            break