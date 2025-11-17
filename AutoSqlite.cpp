//
// Created by Adam.Phoenix on 27/05/2016.
//

#include "AutoSqlite.hpp"

AutoSqlite::AutoSqlite(std::shared_ptr<Sqlite> pSqlite) : mSqlite(pSqlite), mDb(nullptr)
{
	if (nullptr != mSqlite)
	{
		mDb = mSqlite->open();
	}
}

AutoSqlite::~AutoSqlite()
{
	if (nullptr != mSqlite)
	{
		mSqlite->close(mDb);
		mSqlite = nullptr;
		mDb = nullptr;
	}
}

bool AutoSqlite::isOpen()
{
    if ((nullptr != mSqlite) && (nullptr != mDb))
    {
        return true;
    }

    return false;
}

Sqlite* AutoSqlite::get()
{
    return mSqlite.get();
}

char AutoSqlite::getDbType()
{
	if (nullptr != mSqlite)
	{
		return mSqlite->getDbType();
	}

	// default to global db.
	return '0';
}

void AutoSqlite::checkpoint()
{
	if ((nullptr != mSqlite) && (nullptr != mDb))
	{
		return mSqlite->checkpoint(mDb);
	}
}

bool AutoSqlite::query(string& query, void* pClass, void *pArg, QueryCallbackType callbackType, bool bLock)
{
	if ((nullptr != mSqlite) && (nullptr != mDb))
	{
		return mSqlite->query(mDb, query, pClass, pArg, callbackType, bLock);
	}
	return false;
}

bool AutoSqlite::query(const char* table, string& where, bool bLock)
{
	if ((nullptr != mSqlite) && (nullptr != mDb))
	{
		return mSqlite->query(mDb, table, where, bLock);
	}
	return false;
}

QueryState AutoSqlite::query(const char* table, const char* where, field_map& data, bool bLock)
{
	if ((nullptr != mSqlite) && (nullptr != mDb))
	{
		return mSqlite->query(mDb, table, where, data, bLock);
	}
	return ITEM_ERROR;
}

QueryState AutoSqlite::query(const char* table, const string& where, field_map& data, bool bLock)
{
	if ((nullptr != mSqlite) && (nullptr != mDb))
	{
		return mSqlite->query(mDb, table, where, data, bLock);
	}
	return ITEM_ERROR;
}

void AutoSqlite::getValues(Schema* pSchema, std::vector<field_map>& data, const char* where, bool bLock)
{
	if ((nullptr != pSchema) && (nullptr != mSqlite) && (nullptr != mDb))
	{
		return mSqlite->getValues(mDb, pSchema, data, where, bLock);
	}
}

void AutoSqlite::getValues(Schema* pSchema, field_map& data, const char* where, bool bLock)
{
	if ((nullptr != pSchema) && (nullptr != mSqlite) && (nullptr != mDb))
	{
		return mSqlite->getValues(mDb, pSchema, data, where, bLock);
	}
}

string AutoSqlite::getValue(const char* table, const char* valueName, const char* where, bool bLock)
{
	if ((nullptr != mSqlite) && (nullptr != mDb))
	{
		return mSqlite->getValue(mDb, table, valueName, where, bLock);
	}
	return "";
}

string AutoSqlite::getId(const char* table, const char* where, bool bLock)
{
	if ((nullptr != mSqlite) && (nullptr != mDb))
	{
		return mSqlite->getId(mDb, table, where, bLock);
	}
	return "";
}

string AutoSqlite::getId(const char* table, const string& where, bool bLock)
{
	if ((nullptr != mSqlite) && (nullptr != mDb))
	{
		return mSqlite->getId(mDb, table, where, bLock);
	}
	return "";
}

string AutoSqlite::getIdAppendField(const char* table, const string& where, const char *field, const string& value, bool bLock)
{
	if ((nullptr != mSqlite) && (nullptr != mDb))
	{
		return mSqlite->getIdAppendField(mDb, table, where, field, value, bLock);
	}
	return "";
}

bool AutoSqlite::exec(const char* data, bool bLock)
{
	if ((nullptr != mSqlite) && (nullptr != mDb))
	{
		return mSqlite->exec(mDb, data, bLock);
	}
	return false;
}

bool AutoSqlite::exec(const string& data, bool bLock)
{
	if ((nullptr != mSqlite) && (nullptr != mDb))
	{
		return mSqlite->exec(mDb, data, bLock);
	}
	return false;
}

int AutoSqlite::del(const char* table, vector<string>& ids, const char* field, const char* where)
{
    if ((nullptr != mSqlite) && (nullptr != mDb))
    {
        return mSqlite->del(mDb, table, field, where, ids);
    }
    return 0;
}

/**
 * Delete items from the table that are not in the 'ids' list.
 * @param table
 * @param field
 * @param where
 * @param ids
 * @param newids
 * @return A list of items that are in 'ids' but not in the table.
 */
int AutoSqlite::delNotInList(const char* table, const char* field, const char* where, vector<string>& ids, vector<string>& newids)
{
	if ((nullptr != mSqlite) && (nullptr != mDb))
	{
		return mSqlite->delNotInList(mDb, table, field, where, ids, newids);
	}
	return 0;
}

//int AutoSqlite::del(const char* table, const char* field1, const char* field2, const char* value2, vector<string>& ids)
//{
//	if ((nullptr != mSqlite) && (nullptr != mDb))
//	{
//		return mSqlite->del(mDb, table, field1, field2, value2, ids);
//	}
//	return 0;
//}
//
//int AutoSqlite::del(const char* table, const char* where)
//{
//	if ((nullptr != mSqlite) && (nullptr != mDb))
//	{
//		return mSqlite->del(mDb, table, where);
//	}
//	return 0;
//}

bool AutoSqlite::update(const char* table, vector<string>& ids, const char* data)
{
	if ((nullptr != mSqlite) && (nullptr != mDb))
	{
		return mSqlite->update(mDb, table, ids, data);
	}
	return false;
}

int AutoSqlite::prepDel(const char* table, const char* field1, const char* field2, const char* value2, vector<string>& ids)
{
	if ((nullptr != mSqlite) && (nullptr != mDb))
	{
		return mSqlite->prepDel(mDb, table, field1, field2, value2, ids);
	}
	return 0;
}

int AutoSqlite::doDel(const char* table, const char* field1, const char* field2)
{
	if ((nullptr != mSqlite) && (nullptr != mDb))
	{
		return mSqlite->doDel(mDb, table, field1, field2);
	}
	return 0;
}

void AutoSqlite::printTable(const char* table, const char* sel, const char* where)
{
	if ((nullptr != mSqlite) && (nullptr != mDb))
	{
		mSqlite->printTable(mDb, table, sel, where);
	}
}
