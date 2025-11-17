//
// Created by Adam.Phoenix on 27/05/2016.
//

#pragma once

#include "Types.hpp"
#include "Sqlite.hpp"

class Sqlite;

class DLL_EXPORTED AutoSqlite
{
	std::shared_ptr<Sqlite>	mSqlite;
	sqlite3*            	mDb;

public:
	AutoSqlite(std::shared_ptr<Sqlite> pSqlite);
	virtual ~AutoSqlite();

	bool isOpen();

	Sqlite* get();

	char getDbType();

	void checkpoint();

	bool query(string& query, void* pClass, void *pArg, QueryCallbackType callbackType, bool bLock = true);
	bool query(const char* table, string& where, bool bLock = true);
	QueryState query(const char* table, const char* where, field_map& data, bool bLock = true);
	QueryState query(const char* table, const string& where, field_map& data, bool bLock = true);

	void getValues(Schema*, std::vector<field_map>&, const char* where, bool bLock = true);
	void getValues(Schema*, field_map&, const char* where, bool bLock = true);
	string getValue(const char* table, const char* valueName, const char* where, bool bLock = true);
	string getId(const char* table, const char* where, bool bLock = true);
	string getId(const char* table, const string& where, bool bLock = true);
	string getIdAppendField(const char* table, const string& where, const char *field, const string& value, bool bLock = true);

	bool exec(const char* data, bool bLock = true);
	bool exec(const string& data, bool bLock = true);
	int del(const char* table, vector<string>& ids, const char* field = nullptr, const char* where = nullptr);
	int delNotInList(const char* table, const char* field, const char* where, vector<string>& ids, vector<string>& newids);
	//int del(const char* table, const char* field1, const char* field2, const char* value2, vector<string>& ids);
	//int del(const char* table, const char* where);
	bool update(const char* table, vector<string>& ids, const char* data);

	int prepDel(const char* table, const char* field1, const char* field2, const char* value2, vector<string>& ids);
	int doDel(const char* table, const char* field1, const char* field2);

	void printTable(const char* table, const char* sel, const char* where);
};
