//
// Created by Adam.Phoenix on 9/11/2015.
//

#pragma once

#include "Types.hpp"

enum FieldType
{
	UNDEF,
	BOOL,
	LONG,
	INT,
	FLOAT,
	DOUBLE,
	ENUM,
	STRING,
	B91JSON
};

class Field;

typedef MAPTYPE<string, Field> field_map;

class DLL_EXPORTED Field
{
    string mName;
	FieldType mType;
	bool mModified;
	bool mInSchema;
	bool mDbPath;

public:
	Field();
	Field(const Field& copy);

	Field(uint64_t, bool bInSchema = false, bool bModified = false);
	Field(uint64_t, FieldType type, bool bInSchema = false, bool bModified = false);
	Field(double, FieldType type, bool bInSchema = false, bool bModified = false);

	Field(const char* name, bool bInSchema = false, bool bModified = false);
	Field(const string& name, bool bInSchema = false, bool bModified = false);

	Field(string name, string type, bool bInSchema = false, bool bModified = false);
	Field(string name, FieldType type, bool bInSchema = false, bool bModified = false);

	virtual ~Field();

	string get();
    void set(string);
	uint64_t getUint64();
	string getRaw() const;

	string getStore();

	int getInt();
	float getFloat();
	double getDouble();

	string getPrint();

	FieldType getType();
    void setType(FieldType type);

	bool isModified();
	void setModified(bool bModified);

	bool isInSchema();
	void setInSchema(bool bInSchema);

	bool isDbPath();
	void setIsDbPath(bool bDbPath);

	void maskValue(std::string&);

private:

	void setEscapedValue(const std::string);
	void setEscapedValue(const char*);
};

