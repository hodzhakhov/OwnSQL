#include "DB.h"

std::map<std::string, db::DataType> string_to_DataType = {{"INT",     db::DataType::INT},
                                                          {"BOOL",    db::DataType::BOOL},
                                                          {"FLOAT",   db::DataType::FLOAT},
                                                          {"DOUBLE",  db::DataType::FLOAT},
                                                          {"VARCHAR", db::DataType::VARCHAR}};

void db::DropTableParse(db::Database& db, const std::string& input) {
    std::regex drop_regex(R"(DROP TABLE\s+(\w+)\s*)");

    std::smatch match;
    if (std::regex_match(input, match, drop_regex)) {
        std::string name = match[1];
        if (db.DropTable(name)) {
            std::cout << "Table " + name + " dropped\n";
        } else {
            std::cout << "No such table\n";
        }
        return;
    }
    std::cout << "Wrong SQL request\n";
}

void db::CreateTableParse(db::Database& db, const std::string& input) {
    std::regex create_regex(R"(CREATE TABLE\s+([\w_]+)\s*\((.*)\))");

    std::smatch match_out;
    if (std::regex_match(input, match_out, create_regex)) {
        std::string table_name = match_out[1];

        std::string columns = match_out[2];

        std::regex column_regex(
                R"(\s*([\w_]+)\s+(\w+(\(\d+\))?)\s*(PRIMARY KEY)?\s*(NOT NULL)?\s*(FOREIGN KEY)?\s*(,|$))");

        std::sregex_iterator next(columns.begin(), columns.end(), column_regex);
        std::sregex_iterator end;

        std::vector<std::string> column_names;
        std::vector<db::DataType> types;
        std::vector<bool> not_nulls;
        int primary_key = -1;
        int foreign_key = -1;
        int counter = 0;
        while (next != end) {
            std::smatch match = *next;
            std::string column_name = match[1];
            std::string data_type = match[2];
            std::string len = match[3];
            std::string primary = match[4];
            std::string not_null = match[5];
            std::string foreign = match[6];

            if (!len.empty()) {
                data_type = data_type.substr(0, data_type.length() - len.length());
            }

            column_names.push_back(column_name);
            types.push_back(string_to_DataType[data_type]);

            if (!primary.empty()) {
                primary_key = counter;
            }
            if (!foreign.empty()) {
                foreign_key = counter;
            }

            if (!not_null.empty()) {
                not_nulls.push_back(true);
            } else {
                not_nulls.push_back(false);
            }

            ++next;
            ++counter;
        }
        if (db.CreateTable(table_name, column_names, types, primary_key, foreign_key, not_nulls)) {
            std::cout << "Table " + table_name + " created\n";
        } else {
            std::cout << "Table " + table_name + " is already created\n";
        }
    } else {
        std::cout << "Wrong SQL request\n";
    }
}

void db::InsertParse(db::Database& db, const std::string& input) {
    std::regex insert_regex(R"(INSERT INTO\s+([\w_]+)(?:\s*\((.*)\))?\s*VALUES\s*\((.*)\))");

    std::smatch match;
    if (std::regex_match(input, match, insert_regex)) {
        std::string table_name = match[1];

        if (!db.IsTableExist(table_name)) {
            std::cout << "No table " + table_name << "\n";
            return;
        }

        std::string values = match[3];

        std::vector<std::string> parsed_values;
        std::istringstream iss(values);
        std::string value;
        while (std::getline(iss, value, ',')) {
            parsed_values.push_back(value);
        }
        std::vector<db::DataValue> row;
        Table* table = db.GetTable(table_name);

        if (match[2] == "") {
            for (int i = 0; i < parsed_values.size(); ++i) {
                db::DataType type = table->GetColumnType(i);
                if (type == db::DataType::INT) {
                    row.emplace_back(std::stoi(parsed_values[i]));
                } else if (type == db::DataType::FLOAT) {
                    row.emplace_back(std::stof(parsed_values[i]));
                } else if (type == db::DataType::DOUBLE) {
                    row.emplace_back(std::stod(parsed_values[i]));
                } else if (type == db::DataType::BOOL) {
                    if (parsed_values[i] == "true") {
                        row.emplace_back(true);
                    } else {
                        row.emplace_back(false);
                    }
                } else if (type == db::DataType::VARCHAR) {
                    int k = 0;
                    while (parsed_values[i][k] == ' ') {
                        k++;
                    }
                    parsed_values[i] = parsed_values[i].substr(k, parsed_values[i].length() - k);
                    row.emplace_back(parsed_values[i]);
                }
            }
        } else {
            std::vector<std::string> parsed_names;
            std::istringstream names(match[2]);
            std::string name;
            while (std::getline(names, name, ',')) {
                parsed_names.push_back(name);
            }
            for (int i = 0; i < table->GetColumnCount(); i++) {
                row.emplace_back(db::DataType::NONE);
            }
            for (int i = 0; i < parsed_names.size(); i++) {
                int j = table->GetColumnIndex(parsed_names[i]);
                db::DataType type = table->GetColumnType(j);
                if (type == db::DataType::INT) {
                    row[j] = std::stoi(parsed_values[i]);
                } else if (type == db::DataType::FLOAT) {
                    row[j] = std::stof(parsed_values[i]);
                } else if (type == db::DataType::DOUBLE) {
                    row[j] = std::stod(parsed_values[i]);
                } else if (type == db::DataType::BOOL) {
                    if (parsed_values[i] == "true") {
                        row[j] = true;
                    } else {
                        row[j] = false;
                    }
                } else if (type == db::DataType::VARCHAR) {
                    int k = 0;
                    while (parsed_values[i][k] == ' ') {
                        k++;
                    }
                    parsed_values[i] = parsed_values[i].substr(k, parsed_values[i].length() - k);
                    row[j] = parsed_values[i];
                }
            }
        }
        std::vector<bool> not_nulls = table->GetNotNulls();
        for (int i = 0; i < not_nulls.size(); i++) {
            if (not_nulls[i] && std::holds_alternative<db::DataType>(row[i])) {
                std::cout << "Null expression in not null column\n";
                return;
            }
        }
        int index_of_primary = table->GetPrimaryKey();
        for (int i = 0; i < table->GetRowCount(); i++) {
            if (row[index_of_primary] == table->GetRow(i)->GetValue(index_of_primary)) {
                std::cout << "Row with such primary key exists\n";
                return;
            }
        }
        if (db.AddRow(table_name, row)) {
            std::cout << "Row added\n";
        } else {
            std::cout << "Row wasn't added\n";
        }
    } else {
        std::cout << "Wrong SQL request\n" << std::endl;
    }
}

std::vector<int> db::ParseWhere(db::Database& db, const std::string& table_name, const std::string& string_condition) {
    std::string conditions = string_condition;

    std::vector<std::vector<std::string>> or_conditions;
    std::regex or_pattern(R"(\s+OR\s+)");

    std::sregex_token_iterator or_it(conditions.begin(), conditions.end(), or_pattern, -1);
    std::sregex_token_iterator or_end;

    while (or_it != or_end) {
        std::string or_condition = *or_it++;

        std::vector<std::string> and_conditions;
        std::regex and_pattern(R"(\s+AND\s+)");

        std::sregex_token_iterator and_it(or_condition.begin(), or_condition.end(), and_pattern, -1);
        std::sregex_token_iterator and_end;

        while (and_it != and_end) {
            std::string and_condition = *and_it++;
            and_conditions.push_back(and_condition);
        }

        or_conditions.push_back(and_conditions);
    }

    Table* table = db.GetTable(table_name);
    std::vector<std::vector<WhereClause>> parsed_conditions(or_conditions.size());
    std::regex cond(R"(\s*(\w+)\s+(\>|\<|\<\=|\>\=|\=|\!\=)\s+(.*))");
    std::smatch condition;
    for (int i = 0; i < or_conditions.size(); i++) {
        for (int j = 0; j < or_conditions[i].size(); j++) {
            if (std::regex_match(or_conditions[i][j], condition, cond)) {
                std::string column_name = condition[1];
                int index = table->GetColumnIndex(column_name);
                std::string comp = condition[2];
                DataType type = table->GetColumnType(index);
                DataValue value;
                std::string val = condition[3];
                if (type == db::DataType::INT) {
                    value = std::stoi(val);
                } else if (type == db::DataType::FLOAT) {
                    value = std::stof(val);
                } else if (type == db::DataType::DOUBLE) {
                    value = std::stod(val);
                } else if (type == db::DataType::BOOL) {
                    if (val == "true") {
                        value = true;
                    } else {
                        value = false;
                    }
                } else if (type == db::DataType::VARCHAR) {
                    int k = 0;
                    while (val[k] == ' ') {
                        k++;
                    }
                    val = val.substr(k, val.length() - k);
                    value = val;
                }
                parsed_conditions[i].push_back({index, comp, value});
            }
        }
    }
    std::vector<int> indexes;
    for (int i = 0; i < table->GetRowCount(); i++) {
        bool is_all = false;
        for (auto& parsed_condition: parsed_conditions) {
            bool is_in = true;
            for (auto& k: parsed_condition) {
                if (std::holds_alternative<DataType>(k.value)) {
                    if (!std::holds_alternative<DataType>(table->GetRow(i)->GetValue(k.index))) {
                        is_in = false;
                    }
                } else if (std::holds_alternative<std::string>(k.value)) {
                    is_in = is_in && table->GetRow(i)->CompareStr(k.index, std::get<std::string>(k.value), k.comp);
                } else {
                    if (std::holds_alternative<int>(k.value)) {
                        is_in = is_in && table->GetRow(i)->CompareNum(k.index, std::get<int>(k.value), k.comp);
                    } else if (std::holds_alternative<double>(k.value)) {
                        is_in = is_in && table->GetRow(i)->CompareNum(k.index, std::get<double>(k.value), k.comp);
                    } else if (std::holds_alternative<float>(k.value)) {
                        is_in = is_in && table->GetRow(i)->CompareNum(k.index, std::get<float>(k.value), k.comp);
                    } else if (std::holds_alternative<bool>(k.value)) {
                        is_in = is_in && table->GetRow(i)->CompareNum(k.index, std::get<bool>(k.value), k.comp);
                    }
                }
            }
            is_all = is_all || is_in;
        }
        if (is_all) {
            indexes.push_back(i);
        }
    }
    return indexes;
}

void db::SelectParse(db::Database& db, const std::string& input) {
    std::regex select_regex(R"(SELECT\s+(.*)\s+FROM\s+([\w_]+)\s*(WHERE\s+(.*))?((INNER|LEFT|RIGHT)\s+JOIN\s+([\w_]+)\s+ON\s+(.*))?)");
    std::smatch match;

    if (std::regex_match(input, match, select_regex)) {
        std::string select_columns = match[1];
        std::string table_name = match[2];
        std::string conditions = match[4];

        if (!db.IsTableExist(table_name)) {
            std::cout << "No table " + table_name << "\n";
            return;
        }

        Table* table_left = db.GetTable(table_name);
        Table* table;
        std::string join = match[5];
        std::string join_type = match[6];
        std::string join_table_name = match[7];
        std::string join_condition = match[8];
        if (!join.empty()) {
            Table* table_right = db.GetTable(join_table_name);
            table = new Table(table_left, table_right, join_condition, table_name, join_table_name);
        } else {
            table = table_left;
        }

        std::vector<int> rows_indexes;
        if (!conditions.empty() && !join.empty()) {
            std::vector<int> where_indexes = ParseWhere(db, table_name, conditions);
            std::set<int> join_indexes;
            int in = table->number_of_inner_join;
            int lf = table->number_of_left_join;
            int rg = table->number_of_right_join;
            if (join_type == "INNER") {
                for (int i = 0; i < in; i++) {
                    join_indexes.insert(i);
                }
            } else if (join_type == "LEFT") {
                for (int i = 0; i < in + lf; i++) {
                    join_indexes.insert(i);
                }
            } else if (join_type == "RIGHT") {
                for (int i = 0; i < in; i++) {
                    join_indexes.insert(i);
                }
                for (int i = in + lf; i < in + lf + rg; i++) {
                    join_indexes.insert(i);
                }
            }
            for (int where_index: where_indexes) {
                if (join_indexes.find(where_index) != join_indexes.end()) {
                    rows_indexes.push_back(where_index);
                }
            }
        } else if (!conditions.empty()) {
            rows_indexes = ParseWhere(db, table_name, conditions);
        } else if (!join.empty()) {
            int in = table->number_of_inner_join;
            int lf = table->number_of_left_join;
            int rg = table->number_of_right_join;
            if (join_type == "INNER") {
                for (int i = 0; i < in; i++) {
                    rows_indexes.push_back(i);
                }
            } else if (join_type == "LEFT") {
                for (int i = 0; i < in + lf; i++) {
                    rows_indexes.push_back(i);
                }
            } else if (join_type == "RIGHT") {
                for (int i = 0; i < in; i++) {
                    rows_indexes.push_back(i);
                }
                for (int i = in + lf; i < in + lf + rg; i++) {
                    rows_indexes.push_back(i);
                }
            }
        } else {
            for (int i = 0; i < table->GetRowCount(); i++) {
                rows_indexes.push_back(i);
            }
        }

        if (select_columns == "*") {
            std::vector<std::string> names = table->GetNames();
            for (const auto& name: names) {
                std::cout << name << " ";
            }
            std::cout << "\n";
            for (int rows_index: rows_indexes) {
                Row* row = table->GetRow(rows_index);
                for (int j = 0; j < table->GetColumnCount(); ++j) {
                    db::DataType type = table->GetColumnType(j);
                    if (std::holds_alternative<db::DataType>(row->GetValue(j))) {
                        std::cout << "NULL" << " ";
                    } else if (type == db::DataType::INT) {
                        std::cout << std::get<int>(row->GetValue(j)) << " ";
                    } else if (type == db::DataType::FLOAT) {
                        std::cout << std::get<float>(row->GetValue(j)) << " ";
                    } else if (type == db::DataType::DOUBLE) {
                        std::cout << std::get<double>(row->GetValue(j)) << " ";
                    } else if (type == db::DataType::BOOL) {
                        std::cout << std::get<bool>(row->GetValue(j)) << " ";
                    } else if (type == db::DataType::VARCHAR) {
                        std::cout << std::get<std::string>(row->GetValue(j)) << " ";
                    }
                }
                std::cout << "\n";
            }
        } else {
            std::vector<std::string> parsed_columns;
            std::istringstream iss(select_columns);
            std::string column;
            while (std::getline(iss, column, ',')) {
                parsed_columns.push_back(column);
            }
            std::vector<std::string> names = table->GetNames();
            std::vector<int> index_of_names;
            for (const auto& parsed_column: parsed_columns) {
                int it = std::find(names.begin(), names.end(), parsed_column) - names.begin();
                index_of_names.push_back(it);
                std::cout << names[it] << " ";
            }
            std::cout << "\n";
            for (int rows_index: rows_indexes) {
                Row* row = table->GetRow(rows_index);
                for (int j: index_of_names) {
                    db::DataType type = table->GetColumnType(j);
                    if (std::holds_alternative<db::DataType>(row->GetValue(j))) {
                        std::cout << "NULL" << " ";
                    } else if (type == db::DataType::INT) {
                        std::cout << std::get<int>(row->GetValue(j)) << " ";
                    } else if (type == db::DataType::FLOAT) {
                        std::cout << std::get<float>(row->GetValue(j)) << " ";
                    } else if (type == db::DataType::DOUBLE) {
                        std::cout << std::get<double>(row->GetValue(j)) << " ";
                    } else if (type == db::DataType::BOOL) {
                        std::cout << std::get<bool>(row->GetValue(j)) << " ";
                    } else if (type == db::DataType::VARCHAR) {
                        std::cout << std::get<std::string>(row->GetValue(j)) << " ";
                    }
                }
                std::cout << "\n";
            }
        }
        if (!join.empty()) {
            delete table;
        }
    } else {
        std::cout << "Wrong SQL request\n" << std::endl;
    }
}


void db::DeleteParse(db::Database& db, const std::string& input) {
    std::regex delete_regex(R"(DELETE\s+FROM\s+([\w_]+)\s+WHERE\s+(.+))");
    std::smatch match;

    std::string table_name;
    if (std::regex_match(input, match, delete_regex)) {
        table_name = match[1];
        std::string conditions = match[2];

        if (!db.IsTableExist(table_name)) {
            std::cout << "No table " + table_name << "\n";
            return;
        }

        std::vector<int> index_to_delete = ParseWhere(db, table_name, conditions);
        for (int i = 0; i < index_to_delete.size(); i++) {
            index_to_delete[i] -= i;
        }

        db.DeleteRows(table_name, index_to_delete);
        std::cout << "Rows were deleted\n";
    } else {
        std::cout << "Wrong SQL request\n";
    }
}

void db::UpdateParse(Database& db, const std::string& input) {
    std::regex set_regex(R"(UPDATE\s+([\w_]+)\s+SET\s+(.*)\s+WHERE\s+(.*))");
    std::smatch match;

    if (std::regex_match(input, match, set_regex)) {
        std::string table_name = match[1];

        if (!db.IsTableExist(table_name)) {
            std::cout << "No table " + table_name << "\n";
            return;
        }
        Table* table = db.GetTable(table_name);

        std::string update_clause = match[2];
        std::vector<std::string> updates;
        std::istringstream iss(update_clause);
        std::string update;
        while (std::getline(iss, update, ',')) {
            updates.push_back(update);
        }

        std::vector<std::pair<int, DataValue>> values;
        std::regex upd_regex(R"(([\w_]+)\s+=\s+(.*))");
        std::smatch update_match;
        for (const auto & upd : updates) {
            if (std::regex_match(upd, update_match, upd_regex)) {
                std::string column_name = update_match[1];
                int index = table->GetColumnIndex(column_name);
                DataType type = table->GetColumnType(index);
                DataValue value;
                std::string val = update_match[2];
                if (type == db::DataType::INT) {
                    value = std::stoi(val);
                } else if (type == db::DataType::FLOAT) {
                    value = std::stof(val);
                } else if (type == db::DataType::DOUBLE) {
                    value = std::stod(val);
                } else if (type == db::DataType::BOOL) {
                    if (val == "true") {
                        value = true;
                    } else {
                        value = false;
                    }
                } else if (type == db::DataType::VARCHAR) {
                    value = val;
                }
                values.emplace_back(index, value);
            } else {
                std::cout << "Wrong values request\n";
                return;
            }
        }

        std::string conditions = match[3];
        std::vector<int> index_to_update = ParseWhere(db, table_name, conditions);

        for (int i : index_to_update) {
            for (auto & value : values) {
                table->SetRowValue(i, value.first, value.second);
            }
        }
        std::cout << "Values updated\n";
    } else {
        std::cout << "Wrong SQL request\n";
    }
}

void db::SaveDatabase(Database& db, const std::string& input) {
    std::regex save_regex(R"(SAVE\s+TABLE\s+([\w]+\.txt))");
    std::smatch match;
    if (std::regex_match(input, match, save_regex)) {
        std::string filename = match[1];
        std::unordered_map<std::string, Table*> tables = db.GetTables();
        std::ofstream out(filename);
        for (const auto& it: tables) {
            out << it.first << "\n";
            std::vector<std::string> names = it.second->GetNames();
            for (const auto& name: names) {
                out << name << " ";
            }
            out << "\n";
            for (int i = 0; i < it.second->GetRowCount(); i++) {
                Row* row = it.second->GetRow(i);
                for (int j = 0; j < it.second->GetColumnCount(); ++j) {
                    db::DataType type = it.second->GetColumnType(j);
                    if (std::holds_alternative<db::DataType>(row->GetValue(j))) {
                        out << "NULL" << " ";
                    } else if (type == db::DataType::INT) {
                        out << std::get<int>(row->GetValue(j)) << " ";
                    } else if (type == db::DataType::FLOAT) {
                        out << std::get<float>(row->GetValue(j)) << " ";
                    } else if (type == db::DataType::DOUBLE) {
                        out << std::get<double>(row->GetValue(j)) << " ";
                    } else if (type == db::DataType::BOOL) {
                        out << std::get<bool>(row->GetValue(j)) << " ";
                    } else if (type == db::DataType::VARCHAR) {
                        out << std::get<std::string>(row->GetValue(j)) << " ";
                    }
                }
                out << "\n";
            }
            out << "\n";
        }
        std::cout << "Database saved\n";
    } else {
        std::cout << "Wrong SQL request\n";
    }
}
