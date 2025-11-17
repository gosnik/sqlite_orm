//
// Created by Adam.Phoenix on 11/11/2015.
//

#pragma once

#include "Types.hpp"
#include "Field.hpp"

class AutoSqlite;

class Schema
{
    static string               mModelPrefix;
    static string               mModelListPrefix;

	string 						mTable;
	vector<string> 				mCols;
	vector<string> 				mTypes;
	vector<vector<std::string>> mAnos;
	vector<Schema*>				mParents;
    string                      mType;
    bool 						mHasCustomId;

public:
	Schema(string tableStr, string colsStr, string typesStr, string anosStr, string type);
	virtual ~Schema();

    string getType();
	char getDbType();
	const char* getTable();

	string store(AutoSqlite*, field_map& data, bool bPending = false);
	string store(AutoSqlite*, field_map& data, const char*, bool bPending = false);
	void storeNew(AutoSqlite*, field_map& data);

	string parseAndStore(
			AutoSqlite*,
			string&, vector<string>& parent_ano,
			string& parseTime,
			const char* parentId = nullptr, const char* childIdMap = nullptr);

	string parseAndStore(
			AutoSqlite*,
			const Value &rootValue, const Value& values,
			string& parseTime,
			vector<string>& parent_ano, const char* parentId = nullptr, const char* childIdMap = nullptr);

	void getValues(field_map& data, const Value& values, string& parseTime);

	void getValues(
			field_map& data,
			const Value &rootValue, const Value& values,
			string& parseTime,
			vector<string>& parent_ano, const char* parentId, const char* childIdMap);

	void putValue(field_map&, const char* name, std::string value, bool bModified = false);
	void putValue(field_map&, const char* name, const char* value, bool bModified = false);
	void putValue(field_map&, const char* name, int64_t value, bool bModified = false);
    void putValueFloat(field_map&, const char* name, float value, bool bModified = false);

	void getInsert(ostringstream& update, field_map& data, bool bOrIgnore = false);

	void getUpdate(ostringstream& update, field_map& data);
	void getUpdate(ostringstream& update, const char* where, field_map& data);

	void getUpsert(ostringstream& update, field_map& data);
	void getUpsert(ostringstream& update, const char* where, field_map& data);

    bool isUpdateNeeded(field_map&);
	void setUpdateNeeded(field_map&, bool bNeeded = true);

	void initParents();
	void addParent(Schema*);
	void notifyTableChanged(const char*);

	void printValues(const char*, field_map&);

    void serialize(Json::Value& json, field_map& data, std::vector<std::string>& fields);

    void createTable(AutoSqlite*);

private:

    bool getValue(
			field_map& data, string& field, string& type, vector<string>& ano,
			const Value &rootValue, const Value &value,
			string& parseTime,
			vector<string>& parent_ano, const char* parentId = nullptr, const char* childIdMap = nullptr);

    string getAnnotation(const char* annotation, vector<string>& ano_fields);
    bool getAnnotationBool(const char* annotation, vector<string>& ano_fields);

	std::vector<std::string> &split(const std::string &s, char delim, std::vector<std::string> &elems);
	std::vector<std::string> split(const std::string &s, char delim);

    void escapeStr(std::string &source);
    void str_replace( string &s, const string &search, const string &replace);
	void str_replace(string &s, const char *search, const char *replace);
};
