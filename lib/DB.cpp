#include "DB.h"

bool db::Row::CompareStr(int index, const std::string& right, const std::string& comp) {
    if (comp == "=") {
        return std::get<std::string>(values_[index]) == right;
    } else if (comp == "!=") {
        return std::get<std::string>(values_[index]) != right;
    }
    return false;
}

db::DataValue db::Row::GetValue(int index) const {
    return values_[index];
}

db::Row& db::Row::operator+(const Row& other) {
    for (const auto& value: other.values_) {
        values_.push_back(value);
    }
    return *this;
}

void db::Row::SetValue(int index, const DataValue& value) {
    values_[index] = value;
}

db::Table::Table(Table* left_table, Table* right_table, const std::string& condition, const std::string& left_name,
                 const std::string& right_name) {
    std::vector<DataType> left_types = left_table->GetColumnTypes();
    for (auto left_type: left_types) {
        column_types_.push_back(left_type);
    }
    std::vector<DataType> right_types = right_table->GetColumnTypes();
    for (auto right_type: right_types) {
        column_types_.push_back(right_type);
    }
    std::vector<std::string> left_names = left_table->GetNames();
    for (const auto& left: left_names) {
        names_.push_back(left_name + "." + left);
    }
    std::vector<std::string> right_names = right_table->GetNames();
    for (const auto& right: right_names) {
        names_.push_back(right_name + "." + right);
    }
    std::regex cond(R"(\s*(.*)\s+(\=|\!\=)\s+(.*))");
    std::smatch parse_condition;
    if (std::regex_match(condition, parse_condition, cond)) {
        std::string column_left = parse_condition[1];
        column_left = column_left.erase(0, column_left.find('.') + 1);
        std::string operand = parse_condition[2];
        std::string column_right = parse_condition[3];
        column_right = column_right.erase(0, column_right.find('.') + 1);
        int column_left_index = left_table->GetColumnIndex(column_left);
        int column_right_index = right_table->GetColumnIndex(column_right);
        std::set<int> index_of_left;
        std::set<int> index_of_right;
        for (int i = 0; i < left_table->GetRowCount(); i++) {
            for (int j = 0; j < right_table->GetRowCount(); j++) {
                if (operand == "=" && left_table->GetRow(i)->GetValue(column_left_index) ==
                                      right_table->GetRow(j)->GetValue(column_right_index)) {
                    index_of_left.insert(i);
                    index_of_right.insert(j);
                    std::vector<DataValue> row;
                    for (int k = 0; k < left_table->GetColumnCount(); k++) {
                        row.emplace_back(left_table->GetRow(i)->GetValue(k));
                    }
                    for (int k = 0; k < right_table->GetColumnCount(); k++) {
                        row.emplace_back(right_table->GetRow(i)->GetValue(k));
                    }
                    Row* new_row = new Row(row);
                    rows_.push_back(new_row);
                    number_of_inner_join++;
                } else if (operand == "!=" && left_table->GetRow(i)->GetValue(column_left_index) !=
                                              right_table->GetRow(j)->GetValue(column_right_index)) {
                    index_of_left.insert(i);
                    index_of_right.insert(j);
                    std::vector<DataValue> row;
                    for (int k = 0; k < left_table->GetColumnCount(); k++) {
                        row.emplace_back(left_table->GetRow(i)->GetValue(k));
                    }
                    for (int k = 0; k < right_table->GetColumnCount(); k++) {
                        row.emplace_back(right_table->GetRow(i)->GetValue(k));
                    }
                    Row* new_row = new Row(row);
                    rows_.push_back(new_row);
                    number_of_inner_join++;
                }
            }
        }

        for (int i = 0; i < left_table->GetRowCount(); i++) {
            if (index_of_left.find(i) == index_of_left.end()) {
                std::vector<DataValue> row;
                for (int k = 0; k < left_table->GetColumnCount(); k++) {
                    row.emplace_back(left_table->GetRow(i)->GetValue(k));
                }
                for (int k = 0; k < right_table->GetColumnCount(); k++) {
                    row.emplace_back(db::DataType::NONE);
                }
                Row* new_row = new Row(row);
                rows_.push_back(new_row);
                number_of_left_join++;
            }
        }
        for (int i = 0; i < right_table->GetRowCount(); i++) {
            if (index_of_right.find(i) == index_of_right.end()) {
                std::vector<DataValue> row;
                for (int k = 0; k < left_table->GetColumnCount(); k++) {
                    row.emplace_back(db::DataType::NONE);
                }
                for (int k = 0; k < right_table->GetColumnCount(); k++) {
                    row.emplace_back(right_table->GetRow(i)->GetValue(k));
                }
                Row* new_row = new Row(row);
                rows_.push_back(new_row);
                number_of_right_join++;
            }
        }
    }
}

int db::Table::GetPrimaryKey() const {
    return index_of_primary_;
}

const std::vector<db::DataType>& db::Table::GetColumnTypes() const {
    return column_types_;
}

bool db::Table::AddRow(const std::vector<DataValue>& row) {
    if (row.size() != column_types_.size()) {
        return false;
    }
    Row* new_row = new Row(row);
    bool in_table = false;
    for (auto& rows: rows_) {
        if (rows->GetValue(index_of_primary_) == new_row->GetValue(index_of_primary_)) {
            in_table = true;
            break;
        }
    }
    if (!in_table) {
        rows_.push_back(new_row);
        return true;
    } else {
        return false;
    }
}

int db::Table::GetRowCount() const {
    return rows_.size();
}

int db::Table::GetColumnCount() const {
    return column_types_.size();
}

const std::vector<std::string>& db::Table::GetNames() const {
    return names_;
}

db::DataType db::Table::GetColumnType(int column_index) const {
    if (column_index >= column_types_.size()) {
        return DataType::NONE;
    }
    return column_types_[column_index];
}

int db::Table::GetColumnIndex(const std::string& name_of_column) const {
    for (int i = 0; i < names_.size(); i++) {
        if (names_[i] == name_of_column) {
            return i;
        }
    }
    return -1;
}

const std::vector<bool>& db::Table::GetNotNulls() const {
    return not_null;
}

db::Row* db::Table::GetRow(int row_index) const {
    if (row_index >= rows_.size() && row_index < 0) {
        return nullptr;
    }
    return rows_[row_index];
}

void db::Table::DeleteRow(int index) {
    rows_.erase(rows_.begin() + index);
}

void db::Table::SetRowValue(int index_of_row, int index_of_value, const DataValue& value) {
    rows_[index_of_row]->SetValue(index_of_value, value);
}

bool db::Database::CreateTable(const std::string& table_name, const std::vector<std::string>& column_names,
                               const std::vector<DataType>& types, int primary_key, int foreign_key,
                               const std::vector<bool>& not_nulls) {
    if (tables_.find(table_name) != tables_.end()) {
        return false;
    }
    Table* new_table = new Table(types, column_names, primary_key, foreign_key, not_nulls);
    tables_[table_name] = new_table;
    return true;
}

bool db::Database::DropTable(const std::string& table_name) {
    auto it = tables_.find(table_name);
    if (it == tables_.end()) {
        return false;
    } else {
        tables_.erase(it);
        return true;
    }
}

bool db::Database::AddRow(const std::string& table_name, const std::vector<DataValue>& row) {
    auto it = tables_.find(table_name);
    if (it == tables_.end()) {
        return false;
    }
    if (it->second->AddRow(row)) {
        return true;
    } else {
        return false;
    }
}

bool db::Database::IsTableExist(const std::string& name) const {
    if (tables_.find(name) == tables_.end()) {
        return false;
    } else {
        return true;
    }
}

void db::Database::DeleteRows(const std::string& table_name, const std::vector<int>& rows_to_delete) {
    for (int i: rows_to_delete) {
        tables_[table_name]->DeleteRow(i);
    }
}

const std::unordered_map<std::string, db::Table*>& db::Database::GetTables() const {
    return tables_;
}
