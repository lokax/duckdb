#include "duckdb.hpp"

#include <iostream>
using namespace duckdb;

int main() {

	DuckDB db(nullptr);
	Connection con(db);

	con.Query("CREATE TABLE src (a INTEGER)");
	con.Query("INSERT INTO src VALUES (1)");
	con.Query("INSERT INTO src VALUES (2)");
	con.Query("INSERT INTO src VALUES (3)");
	{
		auto result = con.PendingQuery("SELECT * FROM src;", true);
		auto query_result = result->Execute();
		auto result_chunk = query_result->Fetch();
		auto &vectors = result_chunk->data[0];
		auto data = vectors.GetData();
		data[0] = 100;
	}
    {
        auto result = con.Query("SELECT * FROM src;");
        result->Print();
    }

	// result->Print();

	// auto result = con.Query("SELECT a FROM src WHERE a IN (1, 2, 5, 9)");

	// result->Print();

	/**
	    Connection con2(db);
	#if 0
	    auto result = con2.Query("SELECT src.rowid sr, b, test.rowid tr, a FROM src, test WHERE src.b = test.a");
	    result->Print();

	    con.Query("BEGIN TRANSACTION");
	    con.Query("UPDATE src SET b = 10000 WHERE src.rowid = 0 OR src.rowid = 1 OR src.rowid = 3");

	    con2.Query("BEGIN TRANSACTION");
	    result = con2.Query("UPDATE src SET b = 20000 FROM test WHERE src.b = test.a");
	    result->Print();
	#endif

	    con.Query("BEGIN TRANSACTION");
	    con.Query("DELETE FROM test WHERE test.rowid = 0");

	    con2.Query("BEGIN TRANSACTION");
	    auto result = con2.Query("UPDATE test SET a = 20000 WHERE rowid = 0");
	    result->Print();
	<<<<<<< HEAD
	*/
	return 0;
=======
>>>>>>> 4aa7d9569d361fcd133cca868d0cbbf54cc19485
}
