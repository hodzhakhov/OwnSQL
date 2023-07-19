#include "lib/DB.h"

int main() {
    std::string input;
    db::Database db;
    while (true) {
        std::getline(std::cin, input);
        if (input == "exit") {
            break;
        }
        int len = input.find(' ');
        std::string first_word = input.substr(0, len);
        if (first_word == "CREATE") {
            db::CreateTableParse(db, input);
        } else if (first_word == "DROP") {
            db::DropTableParse(db, input);
        } else if (first_word == "INSERT") {
            db::InsertParse(db, input);
        } else if (first_word == "SELECT") {
            db::SelectParse(db, input);
        } else if (first_word == "DELETE") {
            db::DeleteParse(db, input);
        } else if (first_word == "SAVE") {
            db::SaveDatabase(db, input);
        } else if (first_word == "UPDATE") {
            db::UpdateParse(db, input);
        }
    }
    return 0;
}
