#include <iostream>
#include <unordered_map>
#include <string>
#include <vector>
#include <variant>
#include <regex>
#include <map>
#include <sstream>
#include <algorithm>
#include <set>
#include <fstream>

namespace db {
    enum class DataType {
        INT,
        BOOL,
        FLOAT,
        DOUBLE,
        VARCHAR,
        NONE
    };

    using DataValue = std::variant<int, bool, float, double, std::string, DataType>;

    struct WhereClause {
        int index;
        std::string comp;
        DataValue value;
    };

    class Row {
    private:
        std::vector<DataValue> values_;
    public:
        explicit Row(const std::vector<DataValue>& values) : values_(values) {};

        DataValue GetValue(int index) const;

        ~Row() = default;

        template<typename T>
        bool CompareNum(int index, T right, const std::string& comp) {
            if (comp == "<") {
                return std::get<T>(values_[index]) < right;
            } else if (comp == "<=") {
                return std::get<T>(values_[index]) <= right;
            } else if (comp == ">") {
                return std::get<T>(values_[index]) > right;
            } else if (comp == ">=") {
                return std::get<T>(values_[index]) >= right;
            } else if (comp == "=") {
                return std::get<T>(values_[index]) == right;
            } else if (comp == "!=") {
                return std::get<T>(values_[index]) != right;
            }
            return false;
        }

        bool CompareStr(int index, const std::string& right, const std::string& comp);

        Row& operator+(const Row& other);

        void SetValue(int index, const DataValue& value);
    };

    class Table {
    private:
        std::vector<DataType> column_types_;
        std::vector<std::string> names_;
        std::vector<bool> not_null;
        std::vector<Row*> rows_;
        int index_of_primary_ = 0;
        int index_of_foreign_ = -1;
    public:
        Table(const std::vector<DataType>& types, const std::vector<std::string>& names, int primary_key,
              int foreign_key, const std::vector<bool>& not_nulls)
                : column_types_(types),
                  names_(names),
                  index_of_primary_(primary_key),
                  index_of_foreign_(foreign_key),
                  not_null(not_nulls) {};

        Table(const std::vector<DataType>& types, const std::vector<std::string>& names, int primary_key)
                : Table(types, names, primary_key, -1, {false}) {};

        Table(const std::vector<DataType>& types, const std::vector<std::string>& names)
                : Table(types, names, 0, -1, {false}) {};

        Table(Table* left_table, Table* right_table, const std::string& condition, const std::string& left_name,
              const std::string& right_name);

        ~Table() {
            for (auto& row: rows_) {
                delete row;
            }
        }

        int number_of_inner_join = 0;
        int number_of_left_join = 0;
        int number_of_right_join = 0;

        int GetPrimaryKey() const;

        bool AddRow(const std::vector<DataValue>& row);

        int GetRowCount() const;

        int GetColumnCount() const;

        int GetColumnIndex(const std::string& name_of_column) const;

        const std::vector<bool>& GetNotNulls() const;

        const std::vector<std::string>& GetNames() const;

        DataType GetColumnType(int column_index) const;

        Row* GetRow(int row_index) const;

        void DeleteRow(int index);

        const std::vector<DataType>& GetColumnTypes() const;

        void SetRowValue(int index_of_row, int index_of_value, const DataValue& value);
    };

    class Database {
    private:
        std::unordered_map<std::string, Table*> tables_;
    public:
        Database() = default;

        ~Database() {
            for (auto& it: tables_) {
                delete it.second;
            }
        }

        bool CreateTable(const std::string& table_name, const std::vector<std::string>& column_names,
                         const std::vector<DataType>& types, int primary_key, int foreign_key,
                         const std::vector<bool>& not_nulls);

        bool DropTable(const std::string& table_name);

        bool AddRow(const std::string& table_name, const std::vector<DataValue>& row);

        Table* GetTable(const std::string& name) {
            return tables_[name];
        }

        bool IsTableExist(const std::string& name) const;

        void DeleteRows(const std::string& table_name, const std::vector<int>& rows_to_delete);

        const std::unordered_map<std::string, Table*>& GetTables() const;
    };

    void DropTableParse(Database& db, const std::string& input);

    void CreateTableParse(Database& db, const std::string& input);

    void InsertParse(Database& db, const std::string& input);

    std::vector<int> ParseWhere(Database& db, const std::string& table_name, const std::string& condition);

    void SelectParse(Database& db, const std::string& input);

    void DeleteParse(Database& db, const std::string& input);

    void SaveDatabase(Database& db, const std::string& input);

    void UpdateParse(Database& db, const std::string& input);
}
