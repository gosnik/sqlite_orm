//
// Created by Adam.Phoenix on 9/11/2015.
//

#pragma once

#include "Types.hpp"
#include "Mutex.hpp"
#include "AutoLock.hpp"
#include "Api.hpp"
#include "Field.hpp"

extern "C"
{
	#include "sqlite3.h"
}

typedef void (*QueryCallbackType)(void* pClass, void* pArg, sqlite3_stmt *stmt);

enum QueryState
{
	ITEM_ERROR,
	ITEM_NEW,
	ITEM_MODIFIED,
	ITEM_UNMODIFIED
};

class PrepDelArgs
{
public:
	sqlite3* mDb;
	const string mTable;
	const string mField1;
	const string mField2;
	const string mValue2;
	vector<string>& mIds;

	PrepDelArgs(sqlite3*, const char* table, const char* field1, const char* field2, const string& value2, vector<string>& ids);
};

class Schema;

class Sqlite
{
	sqlite3*            mJavaDb;
	string 				mPath;
	string				mKey;
	Mutex               mMutex;
    string              mAccessToken[APIIX_MAX];

public:
	static char getDbType(const std::string path);

	Sqlite(const std::string& path, sqlite3* const db);
	virtual ~Sqlite();

	bool isDbOwner(sqlite3* const db);
	char getDbType();
	const char* getPath();

	void setJavaDb(sqlite3* javaDb);

	sqlite3* open();
	void close(sqlite3*);

	bool checkTables();
	void checkpoint(sqlite3* db);

	void setPassword(const char*);
	void changePassword(const char*);
	void applyPassword(sqlite3* db);
	void createKey(const char*, uint8_t*, int);

	bool query(sqlite3*, string& query, void* pClass, void *pArg, QueryCallbackType callbackType, bool bLock = true);
	bool query(sqlite3*, const char* table, string& where, bool bLock = true);
	QueryState query(sqlite3*, const char* table, const char* where, field_map& data, bool bLock = true);
	QueryState query(sqlite3*, const char* table, const string& where, field_map& data, bool bLock = true);

	void getValues(sqlite3*, Schema*, std::vector<field_map>&, const char* where, bool bLock = true);
	void getValues(sqlite3*, Schema*, field_map&, const char* where, bool bLock = true);
    string getValue(sqlite3*, const char* table, const char* valueName, const char* where, bool bLock = true);
	string getId(sqlite3*, const char* table, const char* where, bool bLock = true);
	string getId(sqlite3*, const char* table, const string& where, bool bLock = true);
	string getIdAppendField(sqlite3*, const char* table, const string& where, const char *field, const string& value, bool bLock = true);

	bool exec(sqlite3*, const char* data, bool bLock = true);
	bool exec(sqlite3*, const string& data, bool bLock = true);

    int del(sqlite3*, const char* table, const char* where);
    int del(sqlite3*, const char* table, const char* field, const char* where, vector<string> ids);
	int delNotInList(sqlite3*, const char* table, const char* field, const char* where, vector<string> ids, vector<string>& newids);

	bool update(sqlite3*, const char* table, vector<string>& ids, const char* data);

	int prepDelCb(void* pArg, sqlite3_stmt *stmt);
	int prepDel(sqlite3*, const char* table, const char* field1, const char* field2, const char* value2, vector<string>& ids);
	int doDel(sqlite3*, const char* table, const char* field1, const char* field2);

	void printTable(sqlite3*, const char* table, const char* sel, const char* where);
};
