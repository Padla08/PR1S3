#include <iostream>
#include <fstream>
#include <sstream>
#include <cstring>
#include <mutex>
#include <string>
#include <regex>
#include "HashTable.h"  
#include "nlohmann/json.hpp"  

using namespace std;
using json = nlohmann::json;

// Самописная структура для хранения вектора
template<typename T>
struct CustVector {
    T* data;  // Указатель на данные
    size_t size;  // Текущий размер вектора
    size_t capacity;  // Вместимость вектора

    CustVector() : data(nullptr), size(0), capacity(0) {}  // Конструктор по умолчанию

    CustVector(const CustVector& other) {  // Конструктор копирования
        size = other.size;
        capacity = other.capacity;
        data = new T[capacity];
        for (size_t i = 0; i < size; ++i) {
            data[i] = other.data[i];
        }
    }

    CustVector& operator=(const CustVector& other) {  // Оператор присваивания
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

    ~CustVector() {  // Деструктор
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

// Структуры для хранения таблицы
struct Table {
    string name;  // Имя таблицы
    CustVector<string> columns;  // Столбцы таблицы
    CustVector<CustVector<string>> rows;  // Строки таблицы
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

string trim(const string& str) {
    size_t first = str.find_first_not_of(' ');
    if (string::npos == first) {
        return str;
    }
    size_t last = str.find_last_not_of(' ');
    return str.substr(first, last - first + 1);
}

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
        CustVector<string> row_data;
        for (const auto& val : row) {
            row_data.push_back(val);
        }
        table.rows.push_back(row_data);
    }
    table.primary_key = j["primary_key"];

    tables.put(table_name, reinterpret_cast<void*>(new Table(table)));  // Добавление таблицы в хеш-таблицу
}

// Загрузка таблицы из CSV
void load_table_csv(const string& table_name) {
    string file_path = table_name + ".csv";
    cout << "Trying to open file: " << file_path << endl;

    ifstream file(file_path);
    if (!file.is_open()) {
        cout << "File not found." << endl;
        return;
    }

    Table table(table_name);
    string line;
    bool first_line = true;

    while (getline(file, line)) {
        istringstream iss(line);
        string value;
        CustVector<string> row;

        while (getline(iss, value, ',')) {
            // Удаляем лишние кавычки
            if (value.front() == '"' && value.back() == '"') {
                value = value.substr(1, value.size() - 2);
            }
            row.push_back(value);
        }

        if (first_line) {
            // Первая строка содержит заголовки столбцов
            table.columns = row;
            first_line = false;
        }
        else {
            // Остальные строки содержат данные
            table.rows.push_back(row);
        }
    }

    // Обновление ID для каждой записи
    for (size_t i = 0; i < table.rows.size; ++i) {
        table.rows[i][0] = to_string(i);  // Обновляем ID
    }

    tables.put(table_name, reinterpret_cast<void*>(new Table(table)));  // Добавление таблицы в хеш-таблицу
    save_table_json(table);  // Сохранение таблицы в JSON
    cout << "Table loaded from " << table_name << ".csv and saved to " << table_name << ".json" << endl;
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

// Функция для сохранения последовательности первичных ключей
void save_pk_sequence(const Table& table) {
    ofstream file(table.name + "_pk_sequence.txt");
    if (!file.is_open()) {
        cout << "Failed to open file for writing pk_sequence." << endl;
        return;
    }
    file << table.pk_sequence;
    cout << "Primary key sequence saved to " << table.name << "_pk_sequence.txt" << endl;
}

// Функция для загрузки последовательности первичных ключей
void load_pk_sequence(Table& table) {
    ifstream file(table.name + "_pk_sequence.txt");
    if (!file.is_open()) {
        cout << "File not found for pk_sequence." << endl;
        return;
    }
    file >> table.pk_sequence;
    cout << "Primary key sequence loaded from " << table.name << "_pk_sequence.txt" << endl;
}

// Функция для сохранения состояния мьютекса
void save_lock_state(const Table& table) {
    ofstream file(table.name + "_lock.txt");
    if (!file.is_open()) {
        cout << "Failed to open file for writing lock state." << endl;
        return;
    }
    file << "locked"; 
    cout << "Lock state saved to " << table.name << "_lock.txt" << endl;
}

// Функция для загрузки состояния мьютекса
void load_lock_state(Table& table) {
    ifstream file(table.name + "_lock.txt");
    if (!file.is_open()) {
        cout << "File not found for lock state." << endl;
        return;
    }
    string state;
    file >> state;
    if (state == "locked") {
        cout << "Lock state loaded from " << table.name << "_lock.txt" << endl;
    }
}

// Функция создания таблицы
void create_table(const string& table_name, const CustVector<string>& columns, const string& primary_key) {
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
    save_pk_sequence(new_table);  // Сохранение последовательности первичных ключей
    save_lock_state(new_table);  // Сохранение состояния мьютекса
    cout << "Table created successfully." << endl;
}

// Функция для выполнения INSERT
void insert_data(const string& table_name, const CustVector<string>& values) {
    Table* table = reinterpret_cast<Table*>(tables.get(table_name));
    if (!table) {
        cout << "Table not found." << endl;
        return;
    }
    lock_guard<mutex> guard(table->lock);  // Блокировка мьютекса для потокобезопасности

    // Проверка на правильное количество значений
    if (values.size != table->columns.size - 1) {  // Уменьшаем на 1, так как первичный ключ добавляется автоматически
        cout << "Invalid number of values." << endl;
        return;
    }

    // Генерация первичного ключа
    size_t last_pk = 0;
    if (table->rows.size > 0) {
        // Находим последний первичный ключ
        last_pk = stoi(table->rows[table->rows.size - 1][0]);
    }
    string pk_value = to_string(last_pk + 1);

    CustVector<string> new_row;
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
    save_pk_sequence(*table);  // Сохранение последовательности первичных ключей
    cout << "Data inserted successfully." << endl;
}

// Функция для выполнения SELECT
void select_data(const CustVector<string>& table_names, const CustVector<string>& columns, const string& condition = "") {
    if (table_names.size == 0) {
        cout << "No tables specified." << endl;
        return;
    }

    // Получаем первую таблицу
    Table* first_table = reinterpret_cast<Table*>(tables.get(table_names[0]));
    if (!first_table) {
        cout << "Table not found: " << table_names[0] << endl;
        return;
    }

    // Если столбцы не указаны, выбираем все столбцы
    CustVector<string> selected_columns = columns;
    if (columns.size == 1 && columns[0] == "*") {
        selected_columns = first_table->columns;
    }

    // Проверка наличия всех столбцов в таблицах
    for (size_t i = 0; i < selected_columns.size; ++i) {
        bool found = false;
        for (size_t j = 0; j < table_names.size; ++j) {
            Table* table = reinterpret_cast<Table*>(tables.get(table_names[j]));
            if (!table) {
                cout << "Table not found: " << table_names[j] << endl;
                return;
            }
            for (size_t k = 0; k < table->columns.size; ++k) {
                if (table->columns[k] == selected_columns[i]) {
                    found = true;
                    break;
                }
            }
            if (found) break;
        }
        if (!found) {
            cout << "Column not found: " << selected_columns[i] << endl;
            return;
        }
    }

    // Вывод данных
    for (size_t i = 0; i < first_table->rows.size; ++i) {
        bool match = true;
        if (!condition.empty()) {
            // Разбор условия
            istringstream iss(condition);
            string col, op, val;
            iss >> col >> op >> val;

            // Удаление лишних символов из значения
            if (val.front() == '(') val = val.substr(1);
            if (val.back() == ')') val = val.substr(0, val.size() - 1);

            for (size_t j = 0; j < first_table->columns.size; ++j) {
                if (first_table->columns[j] == col) {
                    if (op == "=" && first_table->rows[i][j] != val) match = false;
                    if (op == "!=" && first_table->rows[i][j] == val) match = false;
                    break;
                }
            }
        }
        if (match) {
            for (size_t j = 0; j < selected_columns.size; ++j) {
                for (size_t k = 0; k < first_table->columns.size; ++k) {
                    if (first_table->columns[k] == selected_columns[j]) {
                        cout << first_table->rows[i][k] << " ";
                        break;
                    }
                }
            }
            cout << endl;
        }
    }

    // Если есть вторая таблица, выполняем CROSS JOIN
    if (table_names.size > 1) {
        Table* second_table = reinterpret_cast<Table*>(tables.get(table_names[1]));
        if (!second_table) {
            cout << "Table not found: " << table_names[1] << endl;
            return;
        }

        for (size_t i = 0; i < first_table->rows.size; ++i) {
            for (size_t j = 0; j < second_table->rows.size; ++j) {
                bool match = true;
                if (!condition.empty()) {
                    // Разбор условия
                    istringstream iss(condition);
                    string col, op, val;
                    iss >> col >> op >> val;

                    // Удаление лишних символов из значения
                    if (val.front() == '(') val = val.substr(1);
                    if (val.back() == ')') val = val.substr(0, val.size() - 1);

                    for (size_t k = 0; k < first_table->columns.size; ++k) {
                        if (first_table->columns[k] == col) {
                            if (op == "=" && first_table->rows[i][k] != val) match = false;
                            if (op == "!=" && first_table->rows[i][k] == val) match = false;
                            break;
                        }
                    }
                    for (size_t k = 0; k < second_table->columns.size; ++k) {
                        if (second_table->columns[k] == col) {
                            if (op == "=" && second_table->rows[j][k] != val) match = false;
                            if (op == "!=" && second_table->rows[j][k] == val) match = false;
                            break;
                        }
                    }
                }
                if (match) {
                    for (size_t k = 0; k < selected_columns.size; ++k) {
                        for (size_t l = 0; l < first_table->columns.size; ++l) {
                            if (first_table->columns[l] == selected_columns[k]) {
                                cout << first_table->rows[i][l] << " ";
                                break;
                            }
                        }
                        for (size_t l = 0; l < second_table->columns.size; ++l) {
                            if (second_table->columns[l] == selected_columns[k]) {
                                cout << second_table->rows[j][l] << " ";
                                break;
                            }
                        }
                    }
                    cout << endl;
                }
            }
        }
    }
}

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
    regex condition_regex(R"((\w+)\s*(=|<|>|<=|>=|!=)\s*('[^']*'|"[^"]*"|\d+))");
    smatch match;
    if (!regex_search(condition, match, condition_regex)) {
        cout << "Invalid condition format." << endl;
        return;
    }

    string col = match[1];
    string op = match[2];
    string val = match[3];

    // Удаление лишних кавычек из значения
    if (val.front() == '\'' || val.front() == '"') {
        val = val.substr(1, val.size() - 2);
    }

    // Отладочный вывод
    cout << "Parsed condition: col=" << col << ", op=" << op << ", val=" << val << endl;

    CustVector<CustVector<string>> new_rows;
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

    // Переподвес ID
    for (size_t i = 0; i < new_rows.size; ++i) {
        new_rows[i][0] = to_string(i);  // Обновляем ID
    }

    table->rows = new_rows;
    save_table_json(*table);  // Сохранение таблицы в JSON
    save_pk_sequence(*table);  // Сохранение последовательности первичных ключей
    cout << "Rows deleted successfully." << endl;
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
        CustVector<string> columns;
        for (const auto& col : table_json["columns"]) {
            columns.push_back(col);
        }
        string primary_key = table_json["primary_key"];

        Table new_table(table_name);
        new_table.columns = columns;
        new_table.primary_key = primary_key;

        tables.put(table_name, reinterpret_cast<void*>(new Table(new_table)));  // Добавление новой таблицы в хеш-таблицу
        save_table_json(new_table);  // Сохранение таблицы в JSON
        save_pk_sequence(new_table);  // Сохранение последовательности первичных ключей
        save_lock_state(new_table);  // Сохранение состояния мьютекса
        cout << "Table " << table_name << " created successfully." << endl;
    }
}

// Функция для парсинга команд
CustVector<string> parse_command(const string& command) {
    CustVector<string> tokens;
    istringstream iss(command);
    string token;
    bool inside_quotes = false;
    string current_token = "";

    while (iss >> token) {
        if (token.front() == '(' && token.back() == ')') {
            tokens.push_back(token.substr(1, token.size() - 2));
        }
        else if (token.front() == '(') {
            inside_quotes = true;
            current_token += token.substr(1) + " ";
        }
        else if (token.back() == ')') {
            inside_quotes = false;
            current_token += token.substr(0, token.size() - 1);
            tokens.push_back(current_token);
            current_token = "";
        }
        else if (inside_quotes) {
            current_token += token + " ";
        }
        else {
            tokens.push_back(token);
        }
    }

    if (!current_token.empty()) {
        tokens.push_back(current_token);
    }

    return tokens;
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
        CustVector<string> tokens = parse_command(command);
        if (tokens.size == 0) continue;

        if (tokens[0] == "SELECT") {
            if (tokens.size < 4 || tokens[2] != "FROM") {
                cout << "Invalid SELECT command. Usage: SELECT ('column1', 'column2') FROM table_name1, table_name2" << endl;
                continue;
            }
            CustVector<string> columns;
            string columns_str = tokens[1];
            istringstream iss_columns(columns_str);
            string column;
            while (getline(iss_columns, column, ',')) {
                columns.push_back(trim(column));
            }
            CustVector<string> table_names;
            string table_names_str = tokens[3];
            istringstream iss_tables(table_names_str);
            string table_name;
            while (getline(iss_tables, table_name, ',')) {
                table_names.push_back(trim(table_name));
            }
            string condition = "";
            if (tokens.size > 4 && tokens[4] == "WHERE") {
                condition = tokens[5];
            }
            select_data(table_names, columns, condition);
        }
        else if (tokens[0] == "INSERT") {
            if (tokens.size < 4 || tokens[1] != "INTO" || tokens[3] != "VALUES") {
                cout << "Invalid INSERT command. Usage: INSERT INTO table_name VALUES (value1, value2)" << endl;
                continue;
            }
            CustVector<string> values;
            string values_str = tokens[4];
            istringstream iss(values_str);
            string value;
            while (getline(iss, value, ',')) {
                values.push_back(trim(value));
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
            if (tokens.size == 3 && tokens[1] == "TABLE") {
                load_table_json(tokens[2]);
            }
            else if (tokens.size == 3 && tokens[1] == "CSV") {
                load_table_csv(tokens[2]);
            }
            else {
                cout << "Invalid LOAD command. Usage: LOAD TABLE table_name or LOAD CSV table_name" << endl;
            }
        }
        else if (tokens[0] == "CREATE") {
            if (tokens.size < 6 || tokens[1] != "TABLE" || tokens[3] != "(" || tokens[tokens.size - 2] != ")") {
                cout << "Invalid CREATE TABLE command. Usage: CREATE TABLE table_name (column1, column2) PRIMARY KEY (primary_key)" << endl;
                continue;
            }
            CustVector<string> columns;
            string columns_str = tokens[3];
            istringstream iss(columns_str);
            string column;
            while (getline(iss, column, ',')) {
                columns.push_back(trim(column));
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