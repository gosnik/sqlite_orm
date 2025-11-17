//
// Created by Adam.Phoenix on 9/11/2015.
//

#include "Field.hpp"
#include "Utils.hpp"

#define LOG_TAG "NDK"

Field::Field()
        : mName(), mType(UNDEF), mModified(false), mInSchema(false), mDbPath(false)
{

}

Field::Field(const Field& copy)
        : mName(copy.mName), mType(copy.mType), mModified(copy.mModified), mInSchema(copy.mInSchema), mDbPath(copy.mDbPath)
{

}

Field::Field(uint64_t id, bool bInSchema, bool bModified)
	: mName(), mModified(bModified), mInSchema(bInSchema), mDbPath(false)
{
	std::stringstream strstream;
	strstream << "'" << id << "'";
	mName = strstream.str();
	mType = LONG;
}

Field::Field(uint64_t val, FieldType type, bool bInSchema, bool bModified)
		: mName(), mType(type), mModified(bModified), mInSchema(bInSchema), mDbPath(false)
{
	std::stringstream strstream;
	strstream << val;
	mName = strstream.str();
}

Field::Field(double val, FieldType type, bool bInSchema, bool bModified)
		: mName(), mType(type), mModified(bModified), mInSchema(bInSchema), mDbPath(false)
{
	//std::stringstream strstream;
	//strstream << val;
	//mName = strstream.str();
    mName = std::to_string(val);
}

Field::Field(const char* name, bool bInSchema, bool bModified)
		: mName(), mType(STRING), mModified(bModified), mInSchema(bInSchema), mDbPath(false)
{
    setEscapedValue(name);
}

Field::Field(const string& name, bool bInSchema, bool bModified)
		: mName(), mType(STRING), mModified(bModified), mInSchema(bInSchema), mDbPath(false)
{
    setEscapedValue(name);
}

Field::Field(string name, FieldType type, bool bInSchema, bool bModified)
		: mName(), mType(type), mModified(bModified), mInSchema(bInSchema), mDbPath(false)
{
    setEscapedValue(name);
}

Field::Field(string name, string type, bool bInSchema, bool bModified)
		: mName(), mType(UNDEF), mModified(bModified), mInSchema(bInSchema), mDbPath(false)
{
	if (string::npos != type.find("bool"))
	{
		mName = name;
		mType = BOOL;
	}
	else if (string::npos != type.find("long"))
	{
		mName = name;
		mType = LONG;
	}
	////
	else if (string::npos != type.find("int"))
	{
		mName = name;
		mType = INT;
	}
	else if (string::npos != type.find("float"))
	{
		mName = name;
		mType = FLOAT;
	}
	else if (string::npos != type.find("double"))
	{
		mName = name;
		mType = DOUBLE;
	}
	else if (string::npos != type.find("enum"))
	{
        setEscapedValue(name);
		mType = ENUM;
	}
	else if (string::npos != type.find("b91json"))
	{
		mName = name;
		mType = B91JSON;
	}
	else
	{
		setEscapedValue(name);
		mType = STRING;
	}
}

Field::~Field()
{
}

string Field::get()
{
    return mName;
}

void Field::set(string value)
{
    mName = value;
}

void Field::setType(FieldType type)
{
	mType = type;
}

string Field::getRaw() const
{
	string result = mName;
	Utils::stripQuotes(result);
	return result;
}

string Field::getStore()
{
	if (FieldType::B91JSON == mType)
	{
		std::string encoded;
        std::stringstream strstream;
		Utils::encodeBase91(encoded, getRaw(), Format::B91STRING);
        strstream << "'" << encoded << "'";
        return strstream.str();
	}
	else
	{
		return mName;
	}
}

uint64_t Field::getUint64()
{
	uint64_t result = 0;
	string temp = mName;
	Utils::stripQuotes(temp);
	std::istringstream ss(temp);
	ss >> result;

	//LOG_INFO(LOG_GLOBAL, "[Field::getUint64] %s -> %llu", temp.c_str(), result);

	return result;
}

int Field::getInt()
{
	try
	{
		string temp = mName;
		Utils::stripQuotes(temp);
		return std::stoi(temp);
	}
	catch (std::invalid_argument e)
	{

	}

	return 0;
}

float Field::getFloat()
{
	try
	{
		string temp = mName;
		Utils::stripQuotes(temp);
		return std::stof(temp);
	}
	catch (std::invalid_argument e)
	{

	}

	return 0;
}

double Field::getDouble()
{
	try
	{
		string temp = mName;
		Utils::stripQuotes(temp);
		return std::stod(temp);
	}
	catch (std::invalid_argument e)
	{
		std::cout << "[Field::getDouble] exception:" << e.what() << " - from:" << mName << std::endl;
	}

	return 0;
}

FieldType Field::getType()
{
    return mType;
}

bool Field::isModified()
{
    return mModified;
}

void Field::setModified(bool bModified)
{
    mModified = bModified;
}

bool Field::isInSchema()
{
	return mInSchema;
}

void Field::setInSchema(bool bInSchema)
{
	mInSchema = bInSchema;
	//LOG_INFO(LOG_GLOBAL, "[Field] %s in Schema", mName.c_str());
}

bool Field::isDbPath()
{
	return mDbPath;
}

void Field::setIsDbPath(bool bDbPath)
{
	mDbPath = bDbPath;
}

void Field::maskValue(std::string &maskChar)
{
	Utils::maskValue(mName, maskChar);
}

string Field::getPrint()
{
	std::ostringstream dest;
	dest << "[Field] Name:" << mName << " Modified:" << mModified << " InSchema:" << mInSchema;
	return dest.str();
}

void Field::setEscapedValue(const std::string value)
{
    std::string v(value);
    Utils::stripQuotes(v);
    std::stringstream strstream;
    strstream << "'" << v << "'";
    mName = strstream.str();
}

void Field::setEscapedValue(const char* value)
{
    std::string v(value);
    Utils::stripQuotes(v);
    std::stringstream strstream;
    strstream << "'" << v << "'";
    mName = strstream.str();
}
