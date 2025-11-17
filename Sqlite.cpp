//
// Created by Adam.Phoenix on 9/11/2015.
//

#include "Sqlite.hpp"
#include "ORMSQLite.hpp"
#include "Utils.hpp"
#include "prng_engine.h"
#include "Schema.hpp"
#include "AutoSqlite.hpp"

#define LOG_TAG "NDK"

extern "C"
{
    int RegisterExtensionFunctions(sqlite3 *db);
    int RegisterRegexpFunction(sqlite3 *db);
    int busyHandler(void *pArg1, int iPriorCalls)
    {
        // sleep if handler has been called less than threshold value
        if (iPriorCalls < 500)
        {
            // adding a random value here greatly reduces locking
            usleep((rand() % 400000) + 500000);
            return 1;
        }

        // have sqlite3_exec immediately return SQLITE_BUSY
        return 0;
    }
};

Sqlite::Sqlite(const std::string& path, sqlite3* const db)
        : mPath(path), mKey(), mJavaDb(nullptr), mMutex(), mAccessToken()
{
    setJavaDb(db);

    for (int i=0; i<APIIX_MAX; i++)
    {
        mAccessToken[i] = "";
    }
}

Sqlite::~Sqlite()
{
}

bool Sqlite::isDbOwner(sqlite3* const db)
{
    return (db == mJavaDb);
}

const char* Sqlite::getPath()
{
    return mPath.c_str();
}

char Sqlite::getDbType(const std::string path)
{
    char myType = path[path.length() - 4];
    size_t pos = path.find_first_of('?');
    if (string::npos != pos)
    {
        myType = path[pos - 4];
    }

    // get the last character
    return myType;
}

char Sqlite::getDbType()
{
    char myType = mPath[mPath.length() - 4];
    size_t pos = mPath.find_first_of('?');
    if (string::npos != pos)
    {
        myType = mPath[pos - 4];
    }

    // get the last character
    return myType;
}

sqlite3* Sqlite::open()
{
    sqlite3* db = nullptr;

    // Open a new connection to the database.
    // This prevents us from clashing with other threads.
    AutoLock lock(mMutex);
    int errCode = sqlite3_open_v2(
            mPath.c_str(), &db,
            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr);

    if (SQLITE_OK != errCode)
    {
        sqlite3_close(db);
        db = nullptr;
    }

    if (nullptr != db)
    {
        ////LOG_INFO(LOG_SQLITE, "[Sqlite::open] %s", mPath.c_str());
        applyPassword(db);
        exec(db, "PRAGMA read_uncommitted=true;PRAGMA journal_mode=WAL;PRAGMA busy_timeout=5000;");
    }

    return db;
}

void Sqlite::close(sqlite3* db)
{
    if (nullptr != db)
    {
        sqlite3_close(db);
    }
}

void Sqlite::checkpoint(sqlite3* db)
{
    AutoLock lock(mMutex);
    int nLog = 0;
    int nCkpt = 0;
    int rc = sqlite3_wal_checkpoint_v2(
            db,
            nullptr,
            SQLITE_CHECKPOINT_FULL,
            &nLog,
            &nCkpt);

    //LOG_INFO(LOG_SQLITE, "[Sqlite::checkpoint] rc=%d nLog=%d nCkpt=%d", rc, nLog, nCkpt);
}

void Sqlite::setJavaDb(sqlite3* javaDb)
{
    AutoLock lock(mMutex);
    mJavaDb = javaDb;

    applyPassword(mJavaDb);

    if (nullptr != mJavaDb)
    {
        sqlite3_enable_load_extension(mJavaDb, 1);
        //RegisterExtensionFunctions(db);
        RegisterRegexpFunction(mJavaDb);
    }
}

void Sqlite::setPassword(const char* password)
{
    AutoLock lock(mMutex);

    if (nullptr != password)
    {
        mKey = password;
    }
    else
    {
        mKey = "";
    }

    applyPassword(mJavaDb);
}

void Sqlite::changePassword(const char* password)
{
    AutoLock lock(mMutex);

    //LOG_INFO(LOG_SQLITE, "[Sqlite::changePassword]");
    mKey = password;

    if (nullptr != mJavaDb)
    {
        if (Utils::isDebuggerConnected())
            return;
        uint8_t key[32];
        createKey(Utils::decodeStr(mKey.c_str()).c_str(), key, 32);
        sqlite3_rekey_v2(mJavaDb, nullptr, key, 32);
    }
}

void Sqlite::applyPassword(sqlite3* db)
{
    if (mKey.length() > 0 && nullptr != db)
    {
        if (Utils::isDebuggerConnected())
            return;
        //LOG_INFO(LOG_SQLITE, "[Sqlite::applyPassword] set password %s", mPath.c_str());
        uint8_t key[32];
        createKey(Utils::decodeStr(mKey.c_str()).c_str(), key, 32);
        sqlite3_key_v2(db, nullptr, key, 32);
    }
}

bool Sqlite::checkTables()
{
    if (nullptr != mJavaDb)
    {
        string where = "type='table'";
        return query(mJavaDb, "sqlite_master", where);
    }

    return false;
}

void Sqlite::createKey(const char* password, uint8_t* key, int len)
{
    std::ostringstream keyStm;

    // start the key with the supplied password/pin.
    keyStm << password;

    // get the machine id
    //keyStm << ORMSQLite::getInstance()->getDeviceId();

    // use the supplied pin with the machine id to generate a key.
    // here is no upper bound on the size of the key,
    // though only the first 256 bytes (RC4) or 16 bytes (AES128) or 32 bytes (AES256) will be used.
    uint32_t seed = Utils::hash32(keyStm.str().c_str());
    sitmo::prng_engine prng(seed);

    /*
    std::stringstream hexStm;
    hexStm << std::setfill('0') << std::setw(8) << std::hex;

    for (int i=0; i<(32/4); ++i)
    {
        hexStm << prng();
    }
    std::string result = hexStm.str();
    */

    for (int i=0; i<len; i+=4)
    {
        uint32_t val = prng();
        key[i+0] = (val & 0x000000ff);
        key[i+1] = (val & 0x0000ff00) >> 8;
        key[i+2] = (val & 0x00ff0000) >> 16;
        key[i+3] = (val & 0xff000000) >> 24;
    }
}

bool Sqlite::query(sqlite3* db, string& query, void* pClass, void *pArg, QueryCallbackType callbackType, bool bLock/* = true*/)
{
    int rc;
    sqlite3_stmt *stmt;
    bool bFound = false;

    AutoLock lock(mMutex);

    if (nullptr != db)
    {
        //LOG_INFO(LOG_SQLITE, "SQL query: %s", query.c_str());

        if (sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, NULL) == SQLITE_OK)
        {
            while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
            {
                callbackType(pClass, pArg, stmt);
                bFound = true;
            }

            sqlite3_finalize(stmt);
        }
    }

    return bFound;
}

bool Sqlite::query(sqlite3* db, const char* table, string& where, bool bLock/* = true*/)
{
	int rc;
	sqlite3_stmt *stmt;
	bool bFound = false;

	string query = "SELECT * from ";
	query += table;
	query += " WHERE ";
	query += where;

    AutoLock lock(mMutex);

    if (nullptr != db)
    {
        if (sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, NULL) == SQLITE_OK)
        {
            if ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
            {
                bFound = true;
            }

            sqlite3_finalize(stmt);
        }
    }

	return bFound;
}

string Sqlite::getId(sqlite3* db, const char* table, const string& where, bool bLock/* = true*/)
{
    return getId(db, table, where.c_str(), bLock);
}

string Sqlite::getId(sqlite3* db, const char* table, const char* where, bool bLock/* = true*/)
{
    return getValue(db, table, "id", where, bLock);
}

void Sqlite::getValues(sqlite3* db, Schema* pSchema, std::vector<field_map>& data, const char* where, bool bLock)
{
    int rc;
    sqlite3_stmt *stmt;
    string value = "";

    //LOG_INFO(LOG_SQLITE, "[Sqlite::getValues]: %s %s", pSchema->getTable(), where);

    string query = "SELECT * from ";
    query += pSchema->getTable();

    if ((nullptr != where) && (strlen(where) > 0))
    {
        query += " WHERE ";
        query += where;
    }

    AutoLock lock(mMutex);

    if (nullptr != db)
    {
        if (sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, NULL) == SQLITE_OK)
        {
            while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
            {
                field_map row;
                int ncols = sqlite3_column_count(stmt);

                for (int i = 0; i < ncols; i++)
                {
                    const char *col_name = sqlite3_column_name(stmt, i);
                    const char *col_val_c = reinterpret_cast<const char *>(sqlite3_column_text(
                            stmt, i));
                    if (nullptr != col_val_c)
                    {
                        pSchema->putValue(row, col_name, col_val_c);
                    }
                }

                data.push_back(row);
            }

            sqlite3_finalize(stmt);
        }
        else
        {
            //LOG_INFO(LOG_SQLITE, "[Sqlite::getValues]: failed %s %s", pSchema->getTable(), where);
        }
    }
}

void Sqlite::getValues(sqlite3* db, Schema* pSchema, field_map& data, const char* where, bool bLock)
{
    int rc;
    sqlite3_stmt *stmt;
    string value = "";

    //LOG_INFO(LOG_SQLITE, "[Sqlite::getValues]: %s %s", pSchema->getTable(), where);

    string query = "SELECT * from ";
    query += pSchema->getTable();

    if ((nullptr != where) && (strlen(where) > 0))
    {
        query += " WHERE ";
        query += where;
    }

    AutoLock lock(mMutex);
    if (nullptr != db)
    {
        if (sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, NULL) == SQLITE_OK)
        {
            if ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
            {
                int ncols = sqlite3_column_count(stmt);

                for (int i = 0; i < ncols; i++)
                {
                    const char *col_name = sqlite3_column_name(stmt, i);
                    const char *col_val_c = reinterpret_cast<const char *>(sqlite3_column_text(
                            stmt, i));
                    if (nullptr != col_val_c)
                    {
                        //LOG_INFO(LOG_SQLITE, "[Sqlite::getValues]: %s %s %s", pSchema->getTable(), col_name, col_val_c);
                        pSchema->putValue(data, col_name, col_val_c);
                    }
                    else
                    {
                        //LOG_INFO(LOG_SQLITE, "[Sqlite::getValues]: %s %s is NULL", pSchema->getTable(), col_name);
                    }
                }
            }

            sqlite3_finalize(stmt);
        }
        else
        {
            //LOG_INFO(LOG_SQLITE, "[Sqlite::getValues]: failed %s %s", pSchema->getTable(), where);
        }
    }
}

string Sqlite::getValue(sqlite3* db, const char* table, const char* valueName, const char* where, bool bLock/* = true*/)
{
    int rc;
    sqlite3_stmt *stmt;
    string value = "";

    //LOG_INFO(LOG_SQLITE, "[Sqlite::getValue]: %s %s %s", table, valueName, where);

    string query = "SELECT * from ";
    query += table;

    if ((nullptr != where) && (strlen(where) > 0))
    {
        query += " WHERE ";
        query += where;
    }

    AutoLock lock(mMutex);

    if (nullptr != db)
    {
        //LOG_INFO(LOG_SQLITE, "SQL getId: %s", query.c_str());
        if (sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, NULL) == SQLITE_OK)
        {
            if ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
            {
                int ncols = sqlite3_column_count(stmt);

                for (int i = 0; i < ncols; i++)
                {
                    const char *col_name = sqlite3_column_name(stmt, i);

                    if (0 == strcmp(col_name, valueName))
                    {
                        //LOG_INFO(LOG_SQLITE, "SQL get value got col name: %s", col_name);

                        const char *col_val_c = reinterpret_cast<const char *>(sqlite3_column_text(
                                stmt, i));

                        if (nullptr != col_val_c)
                        {
                            value = col_val_c;
                        }

                        break;
                    }
                }
            }

            sqlite3_finalize(stmt);
        }
    }
    return value;
}

string Sqlite::getIdAppendField(sqlite3* db, const char* table, const string& where, const char *field, const string& value, bool bLock/* = true*/)
{
    int rc;
    sqlite3_stmt *stmt;
    string id = "";

    string query = "SELECT * from ";
    query += table;
    query += " WHERE ";
    query += where;

    AutoLock lock(mMutex);

    if (nullptr != db)
    {
        if (sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, NULL) == SQLITE_OK)
        {
            if ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
            {
                int ncols = sqlite3_column_count(stmt);

                for (int i = 0; i < ncols; i++)
                {
                    const char *col_name = sqlite3_column_name(stmt, i);

                    if (0 == strcmp(col_name, "id"))
                    {
                        //LOG_INFO(LOG_SQLITE, "SQL getId got col name: %s", col_name);

                        const char *col_val_c = reinterpret_cast<const char *>(sqlite3_column_text(
                                stmt, i));

                        if (nullptr != col_val_c)
                        {
                            id = col_val_c;
                        }
                    }
                    else if (0 == strcmp(col_name, field))
                    {
                        //LOG_INFO(LOG_SQLITE, "SQL getIdAppendField got col name: %s", col_name);
                        const char *col_val_c = reinterpret_cast<const char *>(sqlite3_column_text(
                                stmt, i));

                        if (nullptr != col_val_c)
                        {
                            string col_val(col_val_c);
                            vector <std::string> v;
                            vector<std::string>::iterator i;
                            bool bFound = false;

                            Utils::split(col_val, ',', v);

                            for (i = v.begin(); i != v.end(); i++)
                            {
                                if (*i == value)
                                {
                                    //LOG_INFO(LOG_SQLITE, "SQL getIdAppendField found value: %s", value.c_str());
                                    bFound = true;
                                    break;
                                }
                            }

                            if (!bFound)
                            {
                                std::ostringstream dest;

                                col_val += "," + value;
                                dest << "UPDATE " << table << " set " << field << "='" << col_val <<
                                        "' WHERE " << where << ";";

                                //LOG_INFO(LOG_SQLITE, "SQL getIdAppendField append value: %s", col_val.c_str());

                                string execStr = dest.str();
                                exec(db, execStr, false);
                            }
                        }
                        else
                        {
                            std::ostringstream dest;

                            dest << "UPDATE " << table << " set " << field << "='" << value <<
                            "' WHERE " << where << ";";

                            //LOG_INFO(LOG_SQLITE, "SQL getIdAppendField append value: %s", value.c_str());

                            string execStr = dest.str();
                            exec(db, execStr, false);
                        }
                    }
                }
            }

            sqlite3_finalize(stmt);
        }
    }
    return id;
}

void Sqlite::printTable(sqlite3* db, const char* table, const char* sel, const char* where)
{
    int rc;
    sqlite3_stmt *stmt;

    AutoLock lock(mMutex);

    if (nullptr != db)
    {
        int ix = 0;
        ostringstream queryStream;
        ostringstream printStream;

        queryStream << "SELECT ";
        if (nullptr != sel)
            queryStream << sel;
        else
            queryStream << " * ";

        queryStream << " FROM " << table;

        if (nullptr != where)
            queryStream << " WHERE " << where;

        string query = queryStream.str();

        if (sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, NULL) == SQLITE_OK)
        {
            while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
            {
                int ncols = sqlite3_column_count(stmt);

                Utils::clear(printStream);

                for (int i = 0; i < ncols; i++)
                {
                    const char *col_val_c = reinterpret_cast<const char *>(sqlite3_column_text(
                            stmt, i));

                    if (nullptr != col_val_c)
                    {
                        printStream << col_val_c;
                    }
                    else
                    {
                        printStream << "NULL";
                    }

                    if (i < ncols-1)
                    {
                        printStream << " , ";
                    }
                }

                LOG_INFO(LOG_SQLITE, "printTable [%s] %s", table, printStream.str().c_str());
            }

            sqlite3_finalize(stmt);
        }
    }
}

QueryState Sqlite::query(sqlite3* db, const char* table, const string& where, field_map& data, bool bLock/* = true*/)
{
    return query(db, table, where.c_str(), data, bLock);
}

QueryState Sqlite::query(sqlite3* db, const char* table, const char* where, field_map& data, bool bLock)
{
	int rc;
	sqlite3_stmt *stmt;
	QueryState state = ITEM_UNMODIFIED;

    if (nullptr != db)
    {
        ostringstream queryStream;
        bool bCompareValues = true;
        bool bFirst = true;

        queryStream << "SELECT ";

        for (field_map::iterator iter = data.begin(); iter != data.end(); ++iter)
        {
            if (!bFirst)
            {
                queryStream << ",";
            }
            queryStream << iter->first;

            bFirst = false;
        }

        queryStream << " FROM ";
        queryStream << table << " WHERE " << where;

        string query = queryStream.str();

        //LOG_INFO(LOG_SQLITE, "SQL query: %s", query.c_str());

        AutoLock lock(mMutex);
        if (sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, NULL) == SQLITE_OK)
        {
            rc = sqlite3_step(stmt);

            if (rc == SQLITE_ROW)
            {
                field_map::iterator it;
                int ncols = sqlite3_column_count(stmt);

                state = ITEM_UNMODIFIED;

                for (int i = 0; i < ncols; i++)
                {
                    const char *col_name = sqlite3_column_name(stmt, i);
                    //LOG_INFO(LOG_SQLITE, "SQL query got col name: %s", col_name);

                    it = data.find(col_name);
                    if (data.end() != it)
                    {
                        //LOG_INFO(LOG_SQLITE, "SQL query got data: %s", it->first.c_str());

                        const char *col_val_c = reinterpret_cast<const char *>(sqlite3_column_text(
                                stmt, i));

                        if (nullptr != col_val_c)
                        {
                            string col_val(col_val_c);
                            if (it->second.getType() == STRING)
                            {
                                std::stringstream strstream;
                                strstream << "'" << col_val_c << "'";
                                col_val = strstream.str();
//                                //LOG_INFO(LOG_SQLITE, "SQL query got data type string");
//                                Utils::str_replace(col_val, "'", "''");
//                                col_val.insert(0, "'");
//                                col_val.insert(col_val.length(), "'");
                            }

                            //LOG_INFO(LOG_SQLITE, "SQL query got col data: %s", col_val.c_str());

                            //LOG_INFO(LOG_SQLITE, "SQL query compare data");
                            if (col_val != it->second.get())
                            {
                                // item updated...
                                it->second.setModified(true);
                                state = ITEM_MODIFIED;

                                //LOG_INFO(LOG_SQLITE, "SQL query modified col: %s %s != %s", col_name,
                                //         col_val.c_str(), it->second.get().c_str());
                            }
                            //LOG_INFO(LOG_SQLITE, "SQL query compared data");
                        }
                        else if (it->second.get().length() > 0)
                        {
                            // item updated...
                            it->second.setModified(true);
                            state = ITEM_MODIFIED;
                        }
                    }
                }
            }
            else if (rc == SQLITE_DONE)
            {
                state = ITEM_NEW;
            }

            sqlite3_finalize(stmt);
        }
    }
	return state;
}
/*
QueryState Sqlite::query(sqlite3* db, const char* table, const char* where, field_map& data, bool bLock)
{
    int rc;
    sqlite3_stmt *stmt;
    QueryState state = ITEM_UNMODIFIED;

    AutoLock lock(mMutex);

    if (nullptr != db)
    {
        bool bFirst = true;
        int ix = 0;
        ostringstream queryStream;

        queryStream << "SELECT ";

        for (field_map::iterator iter = data.begin(); iter != data.end(); ++iter)
        {
            if (!bFirst)
            {
                queryStream << ",";
            }
            queryStream << iter->first;

            bFirst = false;
            ix++;
        }

        queryStream << " FROM ";
        queryStream << table << " WHERE " << where;

        string query = queryStream.str();

        //LOG_INFO(LOG_SQLITE, "[SQL query %p]: %s", db, query.c_str());

        if (sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, NULL) == SQLITE_OK)
        {
            rc = sqlite3_step(stmt);

            if (rc == SQLITE_ROW)
            {
                field_map::iterator it;
                int ncols = sqlite3_column_count(stmt);

                state = ITEM_UNMODIFIED;

                for (int i = 0; i < ncols; i++)
                {
                    const char *col_name = sqlite3_column_name(stmt, i);
                    //LOG_INFO(LOG_SQLITE, "SQL query got col name: %s", col_name);

                    it = data.find(col_name);
                    if (data.end() != it)
                    {
                        //LOG_INFO(LOG_SQLITE, "SQL query got data: %s", it->first.c_str());
                        if (it->second.get().length() > 0)
                        {
                            // item updated...
                            it->second.setModified(true);
                            state = ITEM_MODIFIED;
                        }
                    }
                }
            }
            else if (rc == SQLITE_DONE)
            {
                state = ITEM_NEW;
            }

            sqlite3_finalize(stmt);
        }
    }
    return state;
}
*/

//bool exec(std::ostringstream& data, bool bLock/* = true*/)
//{
//    return exec(db, data.str(), bLock);
//}

bool Sqlite::exec(sqlite3* db, const string& data, bool bLock/* = true*/)
{
    return exec(db, data.c_str(), bLock);
}

bool Sqlite::exec(sqlite3* db, const char* data, bool bLock/* = true*/)
{
    int rc;
    char *zErrMsg = 0;

    if ((nullptr != data) && ((const char)0 != data[0]))
    {
        AutoLock lock(mMutex);
        if (nullptr != db)
        {

            rc = sqlite3_exec(db, data, nullptr, nullptr, &zErrMsg);
            if (rc != SQLITE_OK)
            {
                LOG_ERROR(LOG_GLOBAL, "SQL error exec: %s", data);
                LOG_ERROR(LOG_GLOBAL, "SQL error: %s\n", zErrMsg);
                sqlite3_free(zErrMsg);
                return false;
            }

            return true;
        }
    }
    return false;
}

int Sqlite::del(sqlite3* db, const char* table, const char* field, const char* where, vector<string> ids)
{
    int rc;
    int count = 0;
    AutoLock lock(mMutex);

    if (nullptr == field)
    {
        field = "id";
    }

    if (nullptr != db)
    {
        sqlite3_stmt *stmt;
        std::ostringstream cmdStrm;

        cmdStrm << table << "IDS";
        string idsTable = cmdStrm.str();
        Utils::clear(cmdStrm);

        cmdStrm << table << "IDS." << field;
        string idsTableField = cmdStrm.str();
        Utils::clear(cmdStrm);

        cmdStrm << table << "." << field;
        string tableField = cmdStrm.str();
        Utils::clear(cmdStrm);

        cmdStrm << "CREATE TEMPORARY TABLE " << idsTable << "(" << field << " TEXT);";
        //LOG_INFO(LOG_SQLITE, "[Sqlite::del] %s", cmdStrm.str().c_str());
        exec(db, cmdStrm.str(), false);
        Utils::clear(cmdStrm);

        // Setup a temp table with the ids.
        cmdStrm << "INSERT INTO " << idsTable << " VALUES(@" << field << ")";
        //LOG_INFO(LOG_SQLITE, "[Sqlite::del] %s", cmdStrm.str().c_str());

        sqlite3_prepare_v2(db, cmdStrm.str().c_str(), -1, &stmt, NULL);
        Utils::clear(cmdStrm);

        exec(db, "BEGIN TRANSACTION", false);

        // Ensure the ids are not quoted... cleanup.
        for (std::vector<string>::iterator it = ids.begin(); it != ids.end(); it++)
        {
            string sqlVal1 = (*it);
            Utils::stripQuotes(sqlVal1);
            sqlite3_bind_text(stmt, 1, sqlVal1.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_step(stmt);
            sqlite3_clear_bindings(stmt);
            sqlite3_reset(stmt);
            //LOG_INFO(LOG_SQLITE, "[Sqlite::del] %s", (*it).c_str());
        }

        sqlite3_finalize(stmt);

        exec(db, "END TRANSACTION", false);

        // Setup for performing the delete(s)
        cmdStrm << "DELETE FROM " << table <<
                " WHERE " << tableField << " IN (SELECT " << tableField << " FROM " << idsTable << " )";

        if (nullptr != where)
        {
            cmdStrm << where;
        }
        //LOG_INFO(LOG_SQLITE, "[Sqlite::del] %s", cmdStrm.str().c_str());
        exec(db, cmdStrm.str());
        Utils::clear(cmdStrm);

        // Determine the result of the delete(s)
        count = sqlite3_changes(db);

        // Cleanup temporary table.
        cmdStrm << "DROP TABLE " << idsTable << ";";
        //LOG_INFO(LOG_SQLITE, "[Sqlite::del] %s", cmdStrm.str().c_str());
        exec(db, cmdStrm.str(), false);
        Utils::clear(cmdStrm);
    }
    return count;
}

int Sqlite::delNotInList(sqlite3* db, const char* table, const char* field, const char* where, vector<string> ids, vector<string>& newids)
{
    int rc;
    int count = 0;
    AutoLock lock(mMutex);

    if (nullptr != db)
    {
        sqlite3_stmt *stmt = nullptr;
        std::ostringstream cmdStrm;

        cmdStrm << table << "IDS";
        string idsTable = cmdStrm.str();
        Utils::clear(cmdStrm);

        cmdStrm << table << "IDS." << field;
        string idsTableField = cmdStrm.str();
        Utils::clear(cmdStrm);

        cmdStrm << table << "." << field;
        string tableField = cmdStrm.str();
        Utils::clear(cmdStrm);

        cmdStrm << "CREATE TEMPORARY TABLE " << idsTable << "(" << field << " TEXT);";
        //LOG_INFO(LOG_SQLITE, "[Sqlite::del] %s", cmdStrm.str().c_str());
        exec(db, cmdStrm.str(), false);
        Utils::clear(cmdStrm);

        // Setup a temp table with the ids.
        cmdStrm << "INSERT INTO " << idsTable << " VALUES(@" << field << ")";
        //LOG_INFO(LOG_SQLITE, "[Sqlite::del] %s", cmdStrm.str().c_str());

        sqlite3_prepare_v2(db, cmdStrm.str().c_str(), -1, &stmt, NULL);
        Utils::clear(cmdStrm);

        if (nullptr != stmt)
        {
            exec(db, "BEGIN TRANSACTION", false);

            // Ensure the ids are not quoted... cleanup.
            for (std::vector<string>::iterator it = ids.begin(); it != ids.end(); it++)
            {
                string sqlVal1 = (*it);
                Utils::stripQuotes(sqlVal1);
                sqlite3_bind_text(stmt, 1, sqlVal1.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_step(stmt);
                sqlite3_clear_bindings(stmt);
                sqlite3_reset(stmt);
                //LOG_INFO(LOG_SQLITE, "[Sqlite::del] %s", (*it).c_str());
            }

            sqlite3_finalize(stmt);

            exec(db, "END TRANSACTION", false);

            // Setup for performing the delete(s)
            cmdStrm << "DELETE FROM " << table <<
                    " WHERE " << tableField << " IN (SELECT " << tableField << " FROM " << table <<
                    " LEFT JOIN " << idsTable << " ON " <<
                    tableField << " = " << idsTableField << " WHERE " << idsTableField << " IS NULL)";

            if (nullptr != where)
            {
                cmdStrm << where;
            }
            //LOG_INFO(LOG_SQLITE, "[Sqlite::del] %s", cmdStrm.str().c_str());
            exec(db, cmdStrm.str());
            Utils::clear(cmdStrm);

            // Determine the result of the delete(s)
            count = sqlite3_changes(db);

            // Look for items in ids that are new...
            cmdStrm << "SELECT * FROM " << idsTable <<
                    " WHERE " << idsTableField << " IN (SELECT " << idsTableField << " FROM " << idsTable <<
                    " LEFT JOIN " << table << " ON " <<
                    tableField << " = " << idsTableField << " WHERE " << tableField << " IS NULL)";
            string query = cmdStrm.str();
            if (sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, NULL) == SQLITE_OK)
            {
                const char *col_name = sqlite3_column_name(stmt, 0);

                while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
                {
                    //const char *col_name = sqlite3_column_name(stmt, 0);

                    const char *col_val_c = reinterpret_cast<const char *>(sqlite3_column_text(
                            stmt, 0));

                    // Add new items from ids to newids.
                    if (nullptr != col_val_c)
                    {
                        //string col_val(col_val_c);
                        newids.push_back(col_val_c);
                    }
                }

                sqlite3_finalize(stmt);
            }
            Utils::clear(cmdStrm);
        }
        // Cleanup temporary table.
        cmdStrm << "DROP TABLE " << idsTable << ";";
        //LOG_INFO(LOG_SQLITE, "[Sqlite::del] %s", cmdStrm.str().c_str());
        exec(db, cmdStrm.str(), false);
        Utils::clear(cmdStrm);
    }
    return count;
}

/*
int Sqlite::del(sqlite3* db, const char* table, const char* field1, const char* field2, const char* value2, vector<string>& ids)
{
    int count = 0;

    AutoLock lock(mMutex);

    if (nullptr != db)
    {
        sqlite3_stmt *stmt;
        std::ostringstream cmdStrm;

        cmdStrm << table << "IDS";
        string idsTable = cmdStrm.str();
        Utils::clear(cmdStrm);

        cmdStrm << table << "IDS." << field1;
        string idsTableField1 = cmdStrm.str();
        Utils::clear(cmdStrm);

        cmdStrm << table << "IDS." << field2;
        string idsTableField2 = cmdStrm.str();
        Utils::clear(cmdStrm);

        cmdStrm << table << "." << field1;
        string tableField1 = cmdStrm.str();
        Utils::clear(cmdStrm);

        cmdStrm << table << "." << field2;
        string tableField2 = cmdStrm.str();
        Utils::clear(cmdStrm);

        cmdStrm << "CREATE TEMPORARY TABLE " << idsTable << "(" << field1 << " TEXT," << field2 << " TEXT);";
        //LOG_INFO(LOG_SQLITE, "[Sqlite::del] %s", cmdStrm.str().c_str());
        exec(db, cmdStrm.str(), false);
        Utils::clear(cmdStrm);

        cmdStrm << "INSERT INTO " << idsTable << " (" << field1 << "," << field2 <<") VALUES (?,?)";
        //LOG_INFO(LOG_SQLITE, "[Sqlite::del] %s", cmdStrm.str().c_str());

        sqlite3_prepare_v2(db, cmdStrm.str().c_str(), -1, &stmt, NULL);
        Utils::clear(cmdStrm);

        exec(db, "BEGIN TRANSACTION", false);

        string sqlVal2 = value2;
        Utils::stripQuotes(sqlVal2);

        for (std::vector<string>::iterator it = ids.begin(); it != ids.end(); it++)
        {
            string sqlVal1 = (*it);
            Utils::stripQuotes(sqlVal1);
            sqlite3_bind_text(stmt, 1, sqlVal1.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 2, sqlVal2.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_step(stmt);
            sqlite3_clear_bindings(stmt);
            sqlite3_reset(stmt);
            //LOG_INFO(LOG_SQLITE, "[Sqlite::del] %s %s", sqlVal1.c_str(), sqlVal2.c_str());
        }

        sqlite3_finalize(stmt);

        exec(db, "END TRANSACTION", false);

        cmdStrm << "DELETE FROM " << table <<
        " WHERE " << tableField1 << " IN (SELECT " << tableField1 << " FROM " << table <<
        " LEFT JOIN " << idsTable << " ON " <<
        tableField1 << " = " << idsTableField1 << " AND " << tableField2 << " = " << idsTableField2 <<
        " WHERE " << idsTableField1 <<" IS NULL)";
        //LOG_INFO(LOG_SQLITE, "[Sqlite::del] %s", cmdStrm.str().c_str());
        exec(db, cmdStrm.str());
        Utils::clear(cmdStrm);

        count = sqlite3_changes(db);

        cmdStrm << "DROP TABLE " << idsTable << ";";
        //LOG_INFO(LOG_SQLITE, "[Sqlite::del] %s", cmdStrm.str().c_str());
        exec(db, cmdStrm.str(), false);
        Utils::clear(cmdStrm);

        //int frameCount = 0;
        //int checkpointCount = 0;
        //int result = sqlite3_wal_checkpoint_v2(db, nullptr, SQLITE_CHECKPOINT_PASSIVE, &frameCount, &checkpointCount);

        //cmdStrm << tableField2 << "=" << value2;
        //LOG_INFO(LOG_SQLITE, "[Sqlite::del] print - %s", cmdStrm.str().c_str());
        //printTable(table, "id, groupId, name", nullptr);
    }

    return count;
}
*/

void filterQuery(void* pClass, void* pArg, sqlite3_stmt *stmt)
{
    ((Sqlite*)pClass)->prepDelCb(pArg, stmt);
}

PrepDelArgs::PrepDelArgs(sqlite3* db, const char* table, const char* field1, const char* field2, const string& value2, vector<string>& ids) :
        mDb(db), mTable(table), mField1(field1), mField2(field2), mValue2(value2), mIds(ids)
{

}

int Sqlite::prepDelCb(void* pArg, sqlite3_stmt *stmt)
{
    PrepDelArgs *pArgs = (PrepDelArgs*)pArg;

    //LOG_INFO(LOG_SQLITE, "[Sqlite::prepDelCb] %s", pArgs->mTable.c_str());

    int ncols = sqlite3_column_count(stmt);

    for (int i = 0; i < ncols; i++)
    {
        const char *col_name = sqlite3_column_name(stmt, i);

        //LOG_INFO(LOG_SQLITE, "[Sqlite::prepDelCb] check col:%s", col_name);

        if (0 == strcmp(col_name, pArgs->mField1.c_str()))
        {
            //LOG_INFO(LOG_SQLITE, "[Sqlite::prepDelCb] got col:%s", col_name);
            const char *col_val_c = reinterpret_cast<const char *>(sqlite3_column_text(
                    stmt, i));

            std::ostringstream valStm;
            if ('\'' != col_val_c[0])
            {
                valStm << "'" << col_val_c << "'";
            }
            else
            {
                valStm << col_val_c;
            }
            //string valStr = valStm.str();

            if (nullptr != col_val_c)
            {
                if ( std::find(pArgs->mIds.begin(), pArgs->mIds.end(), valStm.str()) == pArgs->mIds.end() )
                {
                    sqlite3_stmt *insert_stmt;
                    std::ostringstream cmdStrm;

                    cmdStrm << pArgs->mTable << "IDS";
                    string idsTable = cmdStrm.str();
                    Utils::clear(cmdStrm);

                    cmdStrm << "INSERT INTO " << idsTable << " (" << pArgs->mField1 << "," << pArgs->mField2 <<") VALUES (?,?)";
                    //LOG_INFO(LOG_SQLITE, "[Sqlite::prepDelCb] %s", cmdStrm.str().c_str());
                    sqlite3_prepare_v2(pArgs->mDb, cmdStrm.str().c_str(), -1, &insert_stmt, NULL);
                    Utils::clear(cmdStrm);

                    sqlite3_bind_text(insert_stmt, 1, col_val_c, -1, SQLITE_TRANSIENT);
                    sqlite3_bind_text(insert_stmt, 2, pArgs->mValue2.c_str(), -1, SQLITE_TRANSIENT);
                    sqlite3_step(insert_stmt);
                    sqlite3_clear_bindings(insert_stmt);
                    sqlite3_reset(insert_stmt);

                    sqlite3_finalize(insert_stmt);
                    //LOG_INFO(LOG_SQLITE, "[Sqlite::del] %s %s", col_val_c, pArgs->mValue2.c_str());
                }
                else
                {
                    //LOG_INFO(LOG_SQLITE, "[Sqlite::keep] %s %s", col_val_c, pArgs->mValue2.c_str());
                }
            }
        }
    }
}

int Sqlite::prepDel(sqlite3* db, const char* table, const char* field1, const char* field2, const char* value2, vector<string>& ids)
{
    int count = 0;

    AutoLock lock(mMutex);

    if (nullptr != db)
    {
        sqlite3_stmt *stmt;
        std::ostringstream cmdStrm;


        string sqlVal2 = value2;
        Utils::stripQuotes(sqlVal2);

        cmdStrm << table << "IDS";
        string idsTable = cmdStrm.str();
        Utils::clear(cmdStrm);

        cmdStrm << "CREATE TEMPORARY TABLE IF NOT EXISTS " << idsTable << "(" << field1 << " TEXT," << field2 << " TEXT);";
        //LOG_INFO(LOG_SQLITE, "[Sqlite::del] %s", cmdStrm.str().c_str());
        exec(db, cmdStrm.str(), false);
        Utils::clear(cmdStrm);

        PrepDelArgs args(db, table, field1, field2, sqlVal2, ids);
        std::ostringstream queryStrm;
        queryStrm << "select DISTINCT * FROM " << table << " WHERE " << field2 << "=" << value2;
        string queryStr = queryStrm.str();
        query(db, queryStr, this, &args, filterQuery);
    }

    return count;
}

int Sqlite::doDel(sqlite3* db, const char* table, const char* field1, const char* field2)
{
    int count = 0;

    AutoLock lock(mMutex);

    if (nullptr != db)
    {
        std::ostringstream cmdStrm;

        cmdStrm << table << "IDS";
        string idsTable = cmdStrm.str();
        Utils::clear(cmdStrm);

        cmdStrm << table << "IDS." << field1;
        string idsTableField1 = cmdStrm.str();
        Utils::clear(cmdStrm);

        cmdStrm << table << "IDS." << field2;
        string idsTableField2 = cmdStrm.str();
        Utils::clear(cmdStrm);

        cmdStrm << table << "." << field1;
        string tableField1 = cmdStrm.str();
        Utils::clear(cmdStrm);

        cmdStrm << table << "." << field2;
        string tableField2 = cmdStrm.str();
        Utils::clear(cmdStrm);

        cmdStrm << "DELETE FROM " << table <<
        " WHERE " << tableField1 << " IN (SELECT " << tableField1 << " FROM " << table <<
        " LEFT JOIN " << idsTable << " ON " <<
        tableField1 << " = " << idsTableField1 << " AND " << tableField2 << " = " << idsTableField2 <<
        " WHERE " << idsTableField1 <<" IS NOT NULL)";
        //LOG_INFO(LOG_SQLITE, "[Sqlite::del] %s", cmdStrm.str().c_str());
        exec(db, cmdStrm.str());
        Utils::clear(cmdStrm);

        count = sqlite3_changes(db);

        cmdStrm << "DROP TABLE " << idsTable << ";";
        //LOG_INFO(LOG_SQLITE, "[Sqlite::del] %s", cmdStrm.str().c_str());
        exec(db, cmdStrm.str(), false);
        Utils::clear(cmdStrm);

        //cmdStrm << tableField2 << "=" << value2;
        //LOG_INFO(LOG_SQLITE, "[Sqlite::del] print - %s", cmdStrm.str().c_str());
        //printTable(table, "id, groupId, name", nullptr);
    }

    return count;
}

int Sqlite::del(sqlite3* db, const char* table, const char* where)
{
    int count = 0;

    AutoLock lock(mMutex);

    if (nullptr != db)
    {
        sqlite3_stmt *stmt;

        std::ostringstream delCmdStrm;
        delCmdStrm << "DELETE FROM " << table << " WHERE " << where << ";";
        string execCmd = delCmdStrm.str();
        exec(db, execCmd.c_str());
        //LOG_INFO(LOG_SQLITE, "[Sqlite::del] %s", execCmd.c_str());

        count = sqlite3_changes(db);

        //int frameCount = 0;
        //int checkpointCount = 0;
        //int result = sqlite3_wal_checkpoint_v2(db, nullptr, SQLITE_CHECKPOINT_PASSIVE, &frameCount, &checkpointCount);
    }

    return count;
}

bool Sqlite::update(sqlite3* db, const char* table, vector<string>& ids, const char* data)
{
    AutoLock lock(mMutex);

    if (nullptr != db)
    {
        sqlite3_stmt *stmt;
        std::ostringstream updateCmdStm;
        updateCmdStm << "UPDATE  " << table <<
        " set "<< data;

        if(ids.empty())
        {
            updateCmdStm << ";";
            string execCmd = updateCmdStm.str();
            //LOG_INFO(LOG_SQLITE, "SQL updating: %s",execCmd.c_str());
            exec(db, execCmd.c_str());
        }
        else
        {
            exec(db, "CREATE TABLE IDS(id INTEGER);", false);

            sqlite3_prepare_v2(db, "INSERT INTO IDS VALUES(@id)", -1, &stmt, NULL);

            exec(db, "BEGIN TRANSACTION", false);

            for (std::vector<string>::iterator it = ids.begin(); it != ids.end(); it++)
            {
                sqlite3_bind_text(stmt, 1, (*it).c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_step(stmt);
                sqlite3_clear_bindings(stmt);
                sqlite3_reset(stmt);
            }

            sqlite3_finalize(stmt);

            exec(db, "END TRANSACTION", false);

            updateCmdStm <<" WHERE "<< table << ".id IN (SELECT " << table << ".id FROM " << table <<
            " LEFT JOIN IDS ON " <<
            table << ".id = IDS.id WHERE IDS.id IS NULL)";
            string execCmd = updateCmdStm.str();
            //LOG_INFO(LOG_SQLITE, "SQL updating: %s",execCmd.c_str());
            exec(db, execCmd.c_str());

            exec(db, "DROP TABLE IDS;", false);
        }

        return true;
    }

    return false;
}
