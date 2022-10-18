from csv_utils import *
from bd_utils import *


def insert_data_to_main(data_file: list[list], cursor, main_table_name):
    """creates a master table and inserts data into it"""

    result, status = execute_req(cursor, get_req_create_main_table(main_table_name))  # создание главной таблицы.
    if status:  # заполняем только после 1-го создания. в противном случае пропускаем.
        print(status, main_table_name)
        list_title = "(" + ",".join(data_file[0][1:]) + ")"  # формируем шапку для insert.
        list_rows = []
        for each_row in data_file[1:]:  # формируем данные для insert без заголовков.
            list_row = []
            for each_col in each_row[1:-2]:  # исключаем индекс (он же авто).
                list_row.append(each_col.replace("'", "’"))  # заменяем на апострофы, которые не надо экранировать.
            for each_col in each_row[-2:]:  # integer только два последних столбца.
                try:
                    list_row.append(int(each_col))
                except:
                    list_row.append(each_col)

            list_rows.append(str(tuple(list_row)))

        for i in range(200, len(list_rows) + 1, 200):
            req_t = get_req_insert(main_table_name, list_title, list_rows[i - 200:i])  # не берет более 200.
            result, status = execute_req(cursor, req_t)  # добавляем данные в главную таблицу в количестве 200 штук.
            if not status:
                print(i, "failed to add: ",
                      list_rows[i - 1:i])  # выводим строки, которые не добавились.
    else:
        print("the main table has already been created. delete it and you can repeat it")
        return False
    return True


def main():
    """ главный цикл """
    main_table_name = "main_table_animals_0"  # основная таблица из которой будем запросами брать все данные.
    recreate_file("good_animals", "main_animals.csv")  # создаём нормальный good_animals.csv из исходника.

    connection = get_connect_bd()  # попытка подключения к бд.
    if connection:
        cursor = connection.cursor()  # получение курсора.

        data_file = get_read_file("good_animals.csv")  # получение строки нормального файла.
        is_create_main = insert_data_to_main(data_file, cursor,
                                             main_table_name)  # создание главной таблицы с данными. Служит для
        # подтягивания данных для всего остального.

        if is_create_main:
            list_of_table = [(["animal_type"], "animal_types"),  # список таблиц и полей на замену.
                             (["breed"], "breeds"),
                             (["color1", "color2"], "colors"),
                             (["outcome_subtype"], "outcome_subtypes"),
                             (["outcome_type"], "outcome_types")]
            is_all_ok = True
            for each in list_of_table:
                if get_create_dict(cursor, main_table_name, each[0], each[1]):  # создание базовых таблиц.
                    get_update_in_base_table(cursor, main_table_name, each[0], each[1])  # замена индексами таблицы.
                else:
                    is_all_ok = False
            if is_all_ok:
                if create_table_connections(cursor, main_table_name,
                                            "animals"):  # создание связующей таблицы и удаление всего лишнего.

                    # создание ключей +++
                    fks_1 = [['animal', 'animals'], ['outcome_subtype', 'outcome_subtypes'],
                             ['outcome_type', 'outcome_types']]
                    fks_2 = [['animal_type', 'animal_types'], ['breed', 'breeds'], ['color1', 'colors'],
                             ['color2', 'colors']]
                    for each_fk in fks_1:
                        req_t = get_req_append_fk(main_table_name, each_fk[0],
                                                  each_fk[1])  # создание ключей главной таблицы
                        result, status = execute_req(cursor, req_t)
                        print(status, main_table_name)
                    for each_fk in fks_2:
                        req_t = get_req_append_fk("animals", each_fk[0], each_fk[1])  # создание ключей связующей
                        result, status = execute_req(cursor, req_t)
                        print(status, "animals")
                    # создание ключей --

                    print("all operations performed correctly")
                    user_unwer = input("write data to the database? [y/n]:")
                    if user_unwer == "y":
                        connection.commit()
                        print("data recorded in the database")





                else:
                    print("changes are not recorded in the database")
            else:
                print("changes are not recorded in the database")
        else:
            print("changes are not recorded in the database")

        # запросы для создания пользователей в файле create_users.txt

        disconntct_bd(connection, cursor)  # закрытие курсора и подключения к бд


if __name__ == '__main__':
    main()
