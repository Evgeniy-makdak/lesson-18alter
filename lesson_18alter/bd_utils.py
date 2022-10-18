import psycopg2
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def get_connect_bd():  # Подключение к существующей базе данных
    try:
        connection = psycopg2.connect(user="postgres",  # параметры подключения
                                      password="Swaq32123",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="my_bd_animals")
        # connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)  # автокоммит изменений отключил чтоб можно не записывать
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
        return None
    finally:
        print("Успешное подключие к бд")
        print("-" * 30)
    return connection


def disconntct_bd(connection, cursor) -> None:
    cursor.close()
    connection.close()
    print("Соединение с PostgreSQL закрыто")


def execute_req(cursor, req_text: str, is_print: bool = False) -> tuple:
    result = ""
    try:
        cursor.execute(req_text)  # выполнение запроса

        result = cursor.fetchall()
    except:
        # print("не удалось вывести запрос")
        result = ""
    finally:
        if is_print:
            for each in result:
                print(each)
        return result, cursor.statusmessage


def get_req_create_main_table(table_name: str) -> str:
    """ создадим главную таблицу с данными с доп. полями"""
    req = f"""
                create table {table_name}
            (
                id               serial  primary key  unique,
                age_upon_outcome TEXT,
                animal_id        TEXT,
                animal_type      TEXT,
                name             TEXT,
                breed            TEXT,
                color1           TEXT,
                color2           TEXT,
                date_of_birth    timestamp without time zone,
                outcome_subtype  TEXT,
                outcome_type     TEXT,
                outcome_month    INT,
                outcome_year     INT,
                
                id_animal_type   INT,
                id_breed         INT,
                id_color1        INT,
                id_color2        INT,
                
                id_animal          INT,
                id_outcome_subtype INT,
                id_outcome_type    INT
                
            );
                """
    return req


def get_req_create_animals_table(table_name: str) -> str:
    req = f"""
                create table {table_name}
            (
                id  serial primary key unique,
                animal_id        TEXT,
                id_animal_type   INT,
                name             TEXT,
                id_breed         INT,
                id_color1        INT,
                id_color2        INT,
                date_of_birth    timestamp without time zone
                
            );
                """
    return req


def get_req_insert(table_name: str, list_title: str, list_rows: list[str]) -> str:
    list_rows_to_text = ",".join(list_rows)  # сформируем данные в виде строки (),(),(),...

    req = f"""
                INSERT INTO {table_name}{list_title}
                VALUES {list_rows_to_text}; 
                """
    return req


def get_create_dict(cursor, main_table_name: str, column_name: list[str], new_table_name: str) -> bool:
    """ создание базовой(мелкой) таблицы и заполнение уникальными записями из основной
        column_name - список выбранных столбцов
    """
    req_1 = f"""
                create table {new_table_name}
            (
                id  serial  primary key  unique,
                name   TEXT
            );
                """
    result, status = execute_req(cursor, req_1)
    if status:
        print(status, new_table_name)

        if status:  # заполняем, только после создания
            req_2 = f"""
                        INSERT INTO {new_table_name}(name)     
                        SELECT {column_name[0]} from {main_table_name}
                        GROUP BY {column_name[0]}           
                        """
            if len(column_name) > 1:  # если больше одного слолбца для выборки уникальных
                for each_column in column_name[1:]:
                    req_2 += f"""
                    UNION
                    SELECT {each_column} from {main_table_name}
                    GROUP BY {each_column}
                    """
            result, status = execute_req(cursor, req_2)
            if status:
                print(status, new_table_name)
    else:
        return False
    return True


def get_update_in_base_table(cursor, main_table_name: str, column_name: list[str], base_table_name: str) -> None:
    """ заполняем значения основной таблицы индексами базовой таблицы """
    for each_column_name in column_name:
        req_2 = f"""
                    UPDATE {main_table_name}
                    SET id_{each_column_name} = OtherTable.id  
                    FROM (
                        SELECT id, name 
                        FROM {base_table_name}) AS OtherTable
                    WHERE 
                        OtherTable.name = {main_table_name}.{each_column_name}
                    """
        result, status = execute_req(cursor, req_2)
        if status:
            print(status, column_name)


def create_table_connections(cursor, main_table_name: str, new_table_name: str) -> bool:
    result, status = execute_req(cursor, get_req_create_animals_table(new_table_name))  # создание связующей таблицы
    columns = "animal_id,id_animal_type,name,id_breed,id_color1,id_color2,date_of_birth"
    if status:
        print(status, new_table_name)
        # скопируем информацию из общей таблицы
        req_2 = f"""
                    INSERT INTO {new_table_name}({columns})     
                    SELECT {columns} FROM {main_table_name}
                    GROUP BY {columns}           
                    """
        result, status = execute_req(cursor, req_2)
        if status:
            print(status, new_table_name)
            # создадим связи с новой таблицей
            column_name = columns.split(",")
            condition_list = []
            for each_column_name in column_name:
                condition_list.append(f"OtherTable.{each_column_name} = {main_table_name}.{each_column_name}")
            condition = "\n and ".join(condition_list)  # соберем условие из соответсвия нужных полей
            # заполним идешками животного главную таблицу по полному соответсвию полей
            req_2 = f"""
                        UPDATE {main_table_name}
                        SET id_animal = OtherTable.id  
                        FROM (
                            SELECT id,{columns}
                            FROM {new_table_name}) AS OtherTable
                        WHERE 
                            {condition}
                        """
            result, status = execute_req(cursor, req_2)
            if status:
                print(status, main_table_name)
                # добавим в список лишних
                list_id = ["animal_type", "breed", "color1", "color2", "outcome_subtype", "outcome_type"]
                column_name.extend(list_id)

                # удалим лишние столбцы из главной
                for each_column_name in column_name:
                    req_2 = f"""
                                ALTER TABLE {main_table_name} DROP COLUMN {each_column_name};
                                """
                    result, status = execute_req(cursor, req_2)
                    if status:
                        print(status, each_column_name)
                    else:
                        return False
            else:
                return False
        else:
            return False
    return True


def get_req_append_fk(main_table: str, column: str, small_table: str) -> str:
    """ добавление ключей """
    req = f"""
    ALTER TABLE IF EXISTS {main_table}
    ADD CONSTRAINT fk_{column} FOREIGN KEY (id_{column})
    REFERENCES {small_table} (id) MATCH SIMPLE
    """
    return req
