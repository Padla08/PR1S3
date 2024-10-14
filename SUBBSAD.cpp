#include <iostream>
#include <fstream>
#include <sstream>
#include <cstring>
#include <mutex>
#include <string>
#include "HashTable.h"  
#include <nlohmann/json.hpp>  

using namespace std;
using json = nlohmann::json;

// Самописная структура для хранения вектора
template<typename T>
struct MyVector {
    T* data;  // Указатель на данные
    size_t size;  // Текущий размер вектора
    size_t capacity;  // Вместимость вектора

    MyVector() : data(nullptr), size(0), capacity(0) {}  // Конструктор по умолчанию

    MyVector(const MyVector& other) {  // Конструктор копирования
        size = other.size;
        capacity = other.capacity;
        data = new T[capacity];
        for (size_t i = 0; i < size; ++i) {
            data[i] = other.data[i];
        }
    }

    MyVector& operator=(const MyVector& other) {  // Оператор присваивания
        if (this != &other) {
            delete[] data;
            size = other.size;
            capacity = other.capacity;
            data = new T[capacity];
            for (size_t i = 0; i < size; ++i) {
                data[i] = other.data[i];
            }
        }
        return *this;
    }

    ~MyVector() {  // Деструктор
        delete[] data;
    }

    void push_back(const T& value) {  // Добавление элемента в конец вектора
        if (size == capacity) {
            capacity = capacity == 0 ? 1 : capacity * 2;
            T* new_data = new T[capacity];
            for (size_t i = 0; i < size; ++i) {
                new_data[i] = data[i];
            }
            delete[] data;
            data = new_data;
        }
        data[size++] = value;
    }

    T& operator[](size_t index) {  // Оператор доступа по индексу
        return data[index];
    }

    const T& operator[](size_t index) const {  // Константный оператор доступа по индексу
        return data[index];
    }
};

// структуры для хранения таблицы
struct Table {
    string name;  // Имя таблицы
    MyVector<string> columns;  // Столбцы таблицы
    MyVector<MyVector<string>> rows;  // Строки таблицы
    string primary_key;  // Первичный ключ
    size_t pk_sequence;  // Последовательность для первичного ключа
    mutex lock;  // Мьютекс для обеспечения потокобезопасности

    Table(const string& n) : name(n), pk_sequence(0) {}  // Конструктор с именем таблицы

    Table(const Table& other)  // Конструктор копирования
        : name(other.name), columns(other.columns), rows(other.rows), primary_key(other.primary_key), pk_sequence(other.pk_sequence) {}

    Table& operator=(const Table& other) {  // Оператор присваивания
        if (this != &other) {
            name = other.name;
            columns = other.columns;
            rows = other.rows;
            primary_key = other.primary_key;
            pk_sequence = other.pk_sequence;
        }
        return *this;
    }
};

// Карта для хранения таблиц
HashTable tables(10);  // Хеш-таблица для хранения таблиц

// Функция для сохранения данных в JSON
void save_table_json(const Table& table) {
    json j;
    j["name"] = table.name;
    j["columns"] = json::array();
    for (size_t i = 0; i < table.columns.size; ++i) {
        j["columns"].push_back(table.columns[i]);
    }
    j["rows"] = json::array();
    for (size_t i = 0; i < table.rows.size; ++i) {
        json row = json::array();
        for (size_t j = 0; j < table.rows[i].size; ++j) {
            row.push_back(table.rows[i][j]);
        }
        j["rows"].push_back(row);
    }
    j["primary_key"] = table.primary_key;

    ofstream file(table.name + ".json");
    file << j.dump(4);  // Сохранение JSON в файл с отступами для читаемости
}

// Функция для загрузки данных из JSON
void load_table_json(const string& table_name) {
    ifstream file(table_name + ".json");
    if (!file.is_open()) {
        cout << "File not found." << endl;
        return;
    }
    json j;
    file >> j;

    Table table(j["name"]);
    for (const auto& col : j["columns"]) {
        table.columns.push_back(col);
    }
    for (const auto& row : j["rows"]) {
        MyVector<string> row_data;
        for (const auto& val : row) {
            row_data.push_back(val);
        }
        table.rows.push_back(row_data);
    }
    table.primary_key = j["primary_key"];

    tables.put(table_name, reinterpret_cast<void*>(new Table(table)));  // Добавление таблицы в хеш-таблицу
}

// Функция для сохранения таблицы в CSV
void save_table_csv(const Table& table) {
    ofstream file(table.name + ".csv");
    if (!file.is_open()) {
        cout << "Failed to open file for writing." << endl;
        return;
    }

    // Запись заголовков столбцов
    for (size_t i = 0; i < table.columns.size; ++i) {
        file << "\"" << table.columns[i] << "\"";
        if (i < table.columns.size - 1) {
            file << ",";
        }
    }
    file << endl;

    // Запись данных строк
    for (size_t i = 0; i < table.rows.size; ++i) {
        for (size_t j = 0; j < table.rows[i].size; ++j) {
            file << "\"" << table.rows[i][j] << "\"";
            if (j < table.rows[i].size - 1) {
                file << ",";
            }
        }
        file << endl;
    }

    cout << "Table saved to " << table.name << ".csv" << endl;
}

// Функция создания таблицы
void create_table(const string& table_name, const MyVector<string>& columns, const string& primary_key) {
    Table* existing_table = reinterpret_cast<Table*>(tables.get(table_name));
    if (existing_table) {
        cout << "Table already exists." << endl;
        return;
    }

    Table new_table(table_name);
    new_table.columns.push_back(primary_key);  // Добавляем столбец для первичного ключа
    for (size_t i = 0; i < columns.size; ++i) {
        new_table.columns.push_back(columns[i]);
    }
    new_table.primary_key = primary_key;

    tables.put(table_name, reinterpret_cast<void*>(new Table(new_table)));  // Добавление новой таблицы в хеш-таблицу
    save_table_json(new_table);  // Сохранение таблицы в JSON
    cout << "Table created successfully." << endl;
}

// Функция для выполнения SELECT
void select_data(const string& table_name, const MyVector<string>& columns) {
    Table* table = reinterpret_cast<Table*>(tables.get(table_name));
    if (!table) {
        cout << "Table not found." << endl;
        return;
    }
    lock_guard<mutex> guard(table->lock);  // Блокировка мьютекса для потокобезопасности

    // Если столбцы не указаны, выбираем все столбцы
    MyVector<string> selected_columns = columns;
    if (columns.size == 1 && columns[0] == "*") {
        selected_columns = table->columns;
    }

    for (size_t i = 0; i < table->rows.size; ++i) {
        for (size_t j = 0; j < selected_columns.size; ++j) {
            for (size_t k = 0; k < table->columns.size; ++k) {
                if (table->columns[k] == selected_columns[j]) {
                    cout << table->rows[i][k] << " ";
                    break;
                }
            }
        }
        cout << endl;
    }
}

// Функция для выполнения SELECT с фильтрацией
void select_data_with_filter(const string& table_name, const MyVector<string>& columns, const string& condition) {
    Table* table = reinterpret_cast<Table*>(tables.get(table_name));
    if (!table) {
        cout << "Table not found." << endl;
        return;
    }
    lock_guard<mutex> guard(table->lock);  // Блокировка мьютекса для потокобезопасности

    // Если столбцы не указаны, выбираем все столбцы
    MyVector<string> selected_columns = columns;
    if (columns.size == 1 && columns[0] == "*") {
        selected_columns = table->columns;
    }

    // Разбор условия
    istringstream iss(condition);
    string col, op, val, logic_op;
    iss >> col >> op >> val;

    // Удаление лишних символов из значения
    if (val.front() == '(') val = val.substr(1);
    if (val.back() == ')') val = val.substr(0, val.size() - 1);

    for (size_t i = 0; i < table->rows.size; ++i) {
        bool match = false;
        for (size_t j = 0; j < table->columns.size; ++j) {
            if (table->columns[j] == col) {
                if (op == "=" && table->rows[i][j] == val) match = true;
                if (op == "!=" && !(table->rows[i][j] == val)) match = true;
                break;
            }
        }
        if (match) {
            for (size_t j = 0; j < selected_columns.size; ++j) {
                for (size_t k = 0; k < table->columns.size; ++k) {
                    if (table->columns[k] == selected_columns[j]) {
                        cout << table->rows[i][k] << " ";
                        break;
                    }
                }
            }
            cout << endl;
        }
    }
}

// Функция для выполнения INSERT
void insert_data(const string& table_name, const MyVector<string>& values) {
    Table* table = reinterpret_cast<Table*>(tables.get(table_name));
    if (!table) {
        cout << "Table not found." << endl;
        return;
    }
    lock_guard<mutex> guard(table->lock);  // Блокировка мьютекса для потокобезопасности
    if (values.size != table->columns.size - 1) {  // Уменьшаем на 1, так как первичный ключ добавляется автоматически
        cout << "Invalid number of values." << endl;
        return;
    }

    // Генерация первичного ключа
    string pk_value = to_string(table->pk_sequence++);
    MyVector<string> new_row;
    new_row.push_back(pk_value);  // Добавляем первичный ключ в начало строки
    for (size_t i = 0; i < values.size; ++i) {
        // Удаляем лишние символы
        string value = values[i];
        if (value.front() == '(') value = value.substr(1);
        if (value.back() == ')') value = value.substr(0, value.size() - 1);
        new_row.push_back(value);
    }

    table->rows.push_back(new_row);
    save_table_json(*table);  // Сохранение таблицы в JSON
}

// Функция для выполнения DELETE
void delete_data(const string& table_name, const string& condition) {
    Table* table = reinterpret_cast<Table*>(tables.get(table_name));
    if (!table) {
        cout << "Table not found." << endl;
        return;
    }
    lock_guard<mutex> guard(table->lock);  // Блокировка мьютекса для потокобезопасности

    // Проверка на пустую таблицу
    if (table->rows.size == 0) {
        cout << "Table is empty. Nothing to delete." << endl;
        return;
    }

    // Разбор условия
    istringstream iss(condition);
    string col, op, val, logic_op;
    iss >> col >> op >> val;

    // Удаление лишних символов из значения
    if (val.front() == '(') val = val.substr(1);
    if (val.back() == ')') val = val.substr(0, val.size() - 1);

    MyVector<MyVector<string>> new_rows;
    bool any_match = false;

    for (size_t i = 0; i < table->rows.size; ++i) {
        bool match = false;
        for (size_t j = 0; j < table->columns.size; ++j) {
            if (table->columns[j] == col) {
                if (op == "=" && table->rows[i][j] == val) match = true;
                if (op == "!=" && !(table->rows[i][j] == val)) match = true;
                break;
            }
        }
        if (!match) {
            new_rows.push_back(table->rows[i]);
        }
        else {
            any_match = true;
        }
    }

    if (!any_match) {
        cout << "No rows matched the condition. Nothing to delete." << endl;
        return;
    }

    table->rows = new_rows;
    save_table_json(*table);  // Сохранение таблицы в JSON
    cout << "Rows deleted successfully." << endl;
}


// Функция для парсинга комманд
MyVector<string> parse_command(const string& command) {
    MyVector<string> tokens;
    istringstream iss(command);
    string token;
    bool inside_brackets = false;
    string current_token = "";

    while (iss >> token) {
        if (token.front() == '(') {
            inside_brackets = true;
            current_token += token;
        }
        else if (token.back() == ')') {
            inside_brackets = false;
            current_token += " " + token;
            tokens.push_back(current_token);
            current_token = "";
        }
        else if (inside_brackets) {
            current_token += " " + token;
        }
        else {
            tokens.push_back(token);
        }
    }

    return tokens;
}

// Функция для создания таблиц на основе JSON-схемы
void create_tables_from_schema(const string& schema_file) {
    ifstream file(schema_file);
    if (!file.is_open()) {
        cout << "Schema file not found." << endl;
        return;
    }
    json j;
    file >> j;

    for (const auto& table_json : j["tables"]) {
        string table_name = table_json["name"];
        MyVector<string> columns;
        for (const auto& col : table_json["columns"]) {
            columns.push_back(col);
        }
        string primary_key = table_json["primary_key"];

        Table new_table(table_name);
        new_table.columns = columns;
        new_table.primary_key = primary_key;

        tables.put(table_name, reinterpret_cast<void*>(new Table(new_table)));  // Добавление новой таблицы в хеш-таблицу
        save_table_json(new_table);  // Сохранение таблицы в JSON
        cout << "Table " << table_name << " created successfully." << endl;
    }
}

int main() {
    // Создание таблиц на основе JSON-схемы
    create_tables_from_schema("schema.json");

    string temp_command;
    string command;
    while (true) {
        cout << "Enter command: ";
        getline(cin, temp_command);
        command = temp_command;
        MyVector<string> tokens = parse_command(command);
        if (tokens.size == 0) continue;

        if (tokens[0] == "SELECT") {
            if (tokens.size < 4 || tokens[2] != "FROM") {
                cout << "Invalid SELECT command. Usage: SELECT column1, column2 FROM table_name" << endl;
                continue;
            }
            MyVector<string> columns;
            for (size_t i = 1; i < tokens.size - 2; ++i) {
                columns.push_back(tokens[i]);
            }
            if (tokens.size > 4 && tokens[4] == "WHERE") {
                select_data_with_filter(tokens[tokens.size - 2], columns, tokens[tokens.size - 1]);
            }
            else {
                select_data(tokens[tokens.size - 1], columns);
            }
        }
        else if (tokens[0] == "INSERT") {
            if (tokens.size < 4 || tokens[1] != "INTO" || tokens[3] != "VALUES") {
                cout << "Invalid INSERT command. Usage: INSERT INTO table_name VALUES (value1, value2)" << endl;
                continue;
            }
            MyVector<string> values;
            string values_str = tokens[4];
            istringstream iss(values_str);
            string value;
            while (getline(iss, value, ',')) {
                values.push_back(value);
            }
            insert_data(tokens[2], values);
        }
        else if (tokens[0] == "DELETE") {
            if (tokens.size < 4 || tokens[1] != "FROM" || tokens[3] != "WHERE") {
                cout << "Invalid DELETE command. Usage: DELETE FROM table_name WHERE column = value" << endl;
                continue;
            }
            delete_data(tokens[2], tokens[4]);
        }
        else if (tokens[0] == "LOAD") {
            if (tokens.size != 3 || tokens[1] != "TABLE") {
                cout << "Invalid LOAD command. Usage: LOAD TABLE table_name" << endl;
                continue;
            }
            load_table_json(tokens[2]);
        }
        else if (tokens[0] == "CREATE") {
            if (tokens.size < 6 || tokens[1] != "TABLE" || tokens[3] != "(" || tokens[tokens.size - 2] != ")") {
                cout << "Invalid CREATE TABLE command. Usage: CREATE TABLE table_name (column1, column2) PRIMARY KEY (primary_key)" << endl;
                continue;
            }
            MyVector<string> columns;
            string columns_str = tokens[3];
            istringstream iss(columns_str);
            string column;
            while (getline(iss, column, ',')) {
                columns.push_back(column);
            }
            create_table(tokens[2], columns, tokens[tokens.size - 1]);
        }
        else if (tokens[0] == "SAVE") {
            if (tokens.size != 3 || tokens[1] != "TABLE") {
                cout << "Invalid SAVE command. Usage: SAVE TABLE table_name" << endl;
                continue;
            }
            Table* table = reinterpret_cast<Table*>(tables.get(tokens[2]));
            if (!table) {
                cout << "Table not found." << endl;
                continue;
            }
            save_table_csv(*table);
        }
        else if (tokens[0] == "EXIT") {
            break;
        }
        else {
            cout << "Unknown command." << endl;
        }
    }

    return 0;
}