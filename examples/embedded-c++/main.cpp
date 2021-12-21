#include "duckdb.hpp"
#include <iostream>
using namespace duckdb;

int main() {
	std::cout << "hello db" << std::endl;
	DuckDB db("hello-db");
	std::cout << "hello db" << std::endl;
	Connection con(db);

	con.Query("CREATE TABLE integers(i INTEGER)");
	con.Query("INSERT INTO integers VALUES (3)");
	auto result = con.Query("SELECT * FROM integers");
	result->Print();
}
