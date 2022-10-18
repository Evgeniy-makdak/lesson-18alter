import csv


def get_read_file(filename: str) -> list[list]:
    """ считывает файл csv в виде списка [[],[],...]"""
    with open(filename, "r", encoding="utf-8") as file:
        list_row = list(csv.reader(file))
    return list_row


def create_file_bd(filename: str, new_data: list[list]):
    """ создаёт файл filename.csv из списка new_data = [[],[],...]"""
    with open(filename + ".csv", "w", encoding="utf-8",
              newline='') as file:  # newline=''- без пустых строк
        writer = csv.writer(file)
        for each in new_data:
            writer.writerow(tuple(each))

def recreate_file(name: str, filename: str):
    """ создадим нормальный csv (без артефактов) """
    new_list = []
    list_row = get_read_file(filename)
    for each_row in list_row:
        new_row = []
        for each_column in each_row:
            new_row.append(each_column.strip())
        new_list.append(new_row)
    create_file_bd(name, new_list)