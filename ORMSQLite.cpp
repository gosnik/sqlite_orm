//
// Created by aphoenix on 07-Apr-16.
//

#include <sqlite3.h>
#include "ORMSQLite.hpp"
#include "AutoSqlite.hpp"
#include "Schema.hpp"
#include "Base64.hpp"
#include "AutoLock.hpp"
#include <regex>

#pragma message(VAR_NAME_VALUE(BUILDTYPE))

ORMSQLite *ORMSQLite::pThis = nullptr;
JavaVM *ORMSQLite::pVm = nullptr;
jfieldID ORMSQLite::mIntegerFieldId = nullptr;
pthread_mutex_t *thread_locks;

#define LOG_TAG "ORM"

extern unsigned char _binary_AmazonRootCA1_b91_start;
extern unsigned char _binary_AmazonRootCA1_b91_end;
extern unsigned char _binary_AmazonRootCA1_b91_size;

extern unsigned char _binary_AmazonRootCA2_b91_start;
extern unsigned char _binary_AmazonRootCA2_b91_end;
extern unsigned char _binary_AmazonRootCA2_b91_size;

extern unsigned char _binary_AmazonRootCA3_b91_start;
extern unsigned char _binary_AmazonRootCA3_b91_end;
extern unsigned char _binary_AmazonRootCA3_b91_size;

extern unsigned char _binary_AmazonRootCA4_b91_start;
extern unsigned char _binary_AmazonRootCA4_b91_end;
extern unsigned char _binary_AmazonRootCA4_b91_size;

unsigned long openssl_thread_id()
{
    return (unsigned long) pthread_self();
}

void openssl_thread_lock(int mode, int lock_id, const char *file, int line)
{
    if (mode & CRYPTO_LOCK)
        pthread_mutex_lock(&thread_locks[lock_id]);
    else
        pthread_mutex_unlock(&thread_locks[lock_id]);
}

std::string readAsciiFile(const char * path)
{
    std::string result;
    FILE *fp = fopen(path, "r");
    if (fp != NULL)
    {
        fseek(fp, 0L, SEEK_END);
        long size = ftell(fp);
        /* For some reason size comes as zero */
        if (size == 0)
            size = 10000;  /*This will differ for different devices */
        char *buffer = (char *) malloc(size * sizeof(char));
        if (buffer != NULL)
        {
            size_t read = fread(buffer, 1, 10000, fp);
            if (read > 0)
            {
                result = buffer;
            }

            free(buffer);
        }
    }
    else
    {
        LOG_GUARD(std::cout << "Magisk: Failed to read file: " << path << std::endl)
    }

    if (fp != NULL)
        fclose(fp);

    return result;
}

ORMSQLite::ORMSQLite()
        : mMutex(), mCondition(), mInit(false), mInitComplete(false),
          mRun(true), mRunApi(true),
          mBaseUrl(), mBaseFolder(), mDeviceId(),
          mObjMap(), mQueue(), mDbMap(), mApi(),
          mDbPendingMap(), mCertificates(), mSchemas(), mFlags(), mState('\0'),
          mUsername(), mGuestUsername(), mPendingStarted(false), mAAsetMgr(nullptr),
          mAndroidAssetMgr(nullptr)
{
    sqlite3_activate_see(Utils::decodeStr(STR_seeKey).c_str());

    mCommandLine = readAsciiFile("/proc/self/cmdline");

    initNdkDatabase();
    initKeyStore();
    initEtagStore();
}

ORMSQLite::~ORMSQLite()
{
    mRun = false;
    mInit = false;
    mInitComplete = false;

    // cleanup database handles.
    mDbMap.clear();

    // cleanup all of the callback pointer memory.
    pjcallback_map::iterator iter = mObjMap.begin();
    while (iter != mObjMap.end())
    {
        delete iter->second;
        iter++;
    }

    mObjMap.clear();

    pThis = nullptr;
}

ORMSQLite *ORMSQLite::getInstance()
{
    if (nullptr == pThis)
    {
        pThis = new ORMSQLite();
    }
    return pThis;
}

JavaVM *ORMSQLite::getVm()
{
    return pVm;
}

Api &ORMSQLite::getApi()
{
    return mApi;
}

void ORMSQLite::init(const char *baseUrl, const char* baseFolder, AAssetManager *mgr)
{
    if (!mInit)
    {
        bool bSetSslLocks = true;
        size_t num_thread_locks = 0;

        mBaseUrl = baseUrl;
        mBaseFolder = baseFolder;
        mAAsetMgr = mgr;
        mAndroidAssetMgr = new AndroidAssetManager(mAAsetMgr, baseFolder);

        if (!Utils::verifyApk())
        {
            LOG_ERROR(LOG_JNI, "[ORMSQLite::init] verifyApk failed");
            return;
        }

        if (Utils::isDebuggerConnected())
        {
            LOG_ERROR(LOG_JNI, "[ORMSQLite::init] isDebuggerConnected failed");
            return;
        }

        if (Utils::isRootDetected())
        {
            LOG_ERROR(LOG_JNI, "[ORMSQLite::init] isRootDetected failed");
            return;
        }

        for (pschema_map::iterator iter = mSchemas.begin(); iter != mSchemas.end(); ++iter)
        {
            iter->second->initParents();
        }

        // If some other library already set these, don't re-assign
        if (CRYPTO_get_locking_callback() && CRYPTO_get_id_callback())
        {
            bSetSslLocks = false;
        }

        if (bSetSslLocks)
        {
            size_t num_thread_locks = CRYPTO_num_locks();
            thread_locks = (pthread_mutex_t *) calloc(sizeof(pthread_mutex_t), num_thread_locks);
            if (nullptr == thread_locks)
                bSetSslLocks = false;
        }

        if (bSetSslLocks)
        {
            for (size_t i = 0; i < num_thread_locks; ++i)
            {
                if (pthread_mutex_init(&thread_locks[i], nullptr))
                {
                    bSetSslLocks = false;
                    break;
                }
            }

            //TODO: thread cleanup if error...
        }

        if (bSetSslLocks)
        {
            CRYPTO_set_locking_callback(&openssl_thread_lock);
            CRYPTO_set_id_callback(&openssl_thread_id);
        }

        mInit = true;
    }
}

void ORMSQLite::initNdkDatabase()
{
    std::ostringstream dbpath;
    std::string dbpathStr;
    dbpath << "/data/data/" << mCommandLine << "/databases/ndk...db?cache=shared";
    dbpathStr = dbpath.str();

    Sqlite *psqlite = new Sqlite(dbpathStr, nullptr);
    if (nullptr != psqlite)
    {
        //TODO: password protect? psqlite->setPassword("");

        AutoLock lock(mMutex);
        mDbMap[dbpathStr] = std::shared_ptr<Sqlite>(psqlite);
    }
}

void ORMSQLite::postInit()
{
    if (!mInitComplete)
    {
        // get the machine id from java...
        JCallBack *pCb = ORMSQLite::getInstance()->objectFind("getId");
        if (nullptr != pCb)
        {
            AutoJniEnv env;
            AutoJniFrame(env, 2);
            jstring id = (jstring) env->CallObjectMethod(pCb->mObject, pCb->getMethod("getId"));

            if (nullptr != id)
            {
                const char *strId = env->GetStringUTFChars(id, 0);
                mDeviceId = strId;
                env->ReleaseStringUTFChars(id, strId);
                env->DeleteLocalRef(id);
            }
        }

        start();

        mInitComplete = true;
    }
}

//TODO:
void ORMSQLite::signIn()
{

}
void ORMSQLite::setIntegerValue(jobject obj, int value)
{
    AutoJniEnv env;
    env->SetIntField(obj, mIntegerFieldId, value);
}

void ORMSQLite::readCertificates(const char *data, size_t datalen)
{
    stringstream lineBuffer;
    stringstream certBuffer;
    bool bInCertificate = false;
    string decoded;

    if (datalen > 0)
    {
        string encoded(data, datalen);
        Utils::decodeBase91(decoded, encoded);
    }
    else
    {
        string encoded = data;
        Utils::decodeBase91(decoded, encoded);
    }

    size_t len = decoded.length();

    const char *firstChar = decoded.c_str();
    const char *lastChar = (const char *) memchr(firstChar, '\n', len);
    while (nullptr != lastChar)
    {
        lineBuffer.write(firstChar, (lastChar - firstChar) + 1);
        lineBuffer.flush();

        //LOG_INFO(LOG_JNI, "[ORMSQLite::readCertificates] len: %s", lineBuffer.str().c_str());

        if (!bInCertificate)
        {
            if (string::npos != lineBuffer.str().find("-----BEGIN CERTIFICATE-----"))
            {
                //LOG_INFO(LOG_JNI, "[ORMSQLite::readCertificates] got cert");
                bInCertificate = true;
                certBuffer << lineBuffer.rdbuf();
            }
        }
        else
        {
            certBuffer << lineBuffer.rdbuf();
            if (string::npos != lineBuffer.str().find("-----END CERTIFICATE-----"))
            {
                bInCertificate = false;
                certBuffer.flush();
                //LOG_INFO(LOG_JNI, "[ORMSQLite::readCertificates]\n%s", certBuffer.str().c_str());
                //LOG_INFO(LOG_JNI, "[ORMSQLite::readCertificates] push back");
                mCertificates.push_back(certBuffer.str());
                Utils::clear(certBuffer);
            }
        }

        Utils::clear(lineBuffer);

        if (lastChar - firstChar + 1 < len)
        {
            len -= (lastChar - firstChar) + 1;
            firstChar = lastChar + 1;
            lastChar = (const char *) memchr(firstChar, '\n', len);
            //LOG_INFO(LOG_JNI, "[ORMSQLite::readCertificates] nextline remaining %d", len);
        }
        else
        {
            len = 0;
            lastChar = nullptr;
        }
    }
}

void ORMSQLite::readCertificateFile(const char *fileName)
{
    if (nullptr != mAAsetMgr)
    {
        //LOG_INFO(LOG_JNI, "[ORMSQLite::readCertificates] %s", fileName);

        AAsset *pFile = AAssetManager_open(mAAsetMgr, fileName, AASSET_MODE_RANDOM);

        if (nullptr != pFile)
        {
            char buffer[1024];
            stringstream lineBuffer;
            stringstream certBuffer;
            bool bInCertificate = false;

            //LOG_INFO(LOG_JNI, "[ORMSQLite::readCertificates] got cert file");

            int read = AAsset_read(pFile, buffer, 1024);
            //LOG_INFO(LOG_JNI, "[ORMSQLite::readCertificates] begin remaining %d", read);
            while (read > 0)
            {
                char *firstChar = buffer;
                char *lastChar = (char *) memchr(firstChar, '\n', read);
                while (nullptr != lastChar)
                {
                    lineBuffer.write(firstChar, (lastChar - firstChar) + 1);
                    lineBuffer.flush();

                    //LOG_INFO(LOG_JNI, "[ORMSQLite::readCertificates] read: %s", lineBuffer.str().c_str());

                    if (!bInCertificate)
                    {
                        if (string::npos != lineBuffer.str().find("-----BEGIN CERTIFICATE-----"))
                        {
                            //LOG_INFO(LOG_JNI, "[ORMSQLite::readCertificates] got cert");
                            bInCertificate = true;
                            certBuffer << lineBuffer.rdbuf();
                        }
                    }
                    else
                    {
                        certBuffer << lineBuffer.rdbuf();
                        if (string::npos != lineBuffer.str().find("-----END CERTIFICATE-----"))
                        {
                            bInCertificate = false;
                            certBuffer.flush();
                            //LOG_INFO(LOG_JNI, "[ORMSQLite::readCertificates]\n%s", certBuffer.str().c_str());
                            //LOG_INFO(LOG_JNI, "[ORMSQLite::readCertificates] push back");
                            mCertificates.push_back(certBuffer.str());
                            Utils::clear(certBuffer);
                        }
                    }

                    Utils::clear(lineBuffer);

                    if (lastChar - firstChar + 1 < read)
                    {
                        read -= (lastChar - firstChar) + 1;
                        firstChar = lastChar + 1;
                        lastChar = (char *) memchr(firstChar, '\n', read);
                        //LOG_INFO(LOG_JNI, "[ORMSQLite::readCertificates] nextline remaining %d", read);
                    }
                    else
                    {
                        read = 0;
                        lastChar = nullptr;
                    }
                }

                if (read > 0)
                {
                    lineBuffer.write(firstChar, read);
                    //LOG_INFO(LOG_JNI, "[ORMSQLite::readCertificates] temp: %s", lineBuffer.str().c_str());
                }

                // read next block from file.
                read = AAsset_read(pFile, buffer, 1024);
                //LOG_INFO(LOG_JNI, "[ORMSQLite::readCertificates] next remaining %d", read);
            }

            //LOG_INFO(LOG_JNI, "[ORMSQLite::readCertificates] close file");
            AAsset_close(pFile);
        }
        else
        {
            LOG_ERROR(LOG_GLOBAL, "[ORMSQLite::readCertificates] failed to open: %s", fileName);
        }
    }
}

std::vector <string> &ORMSQLite::getCertificates()
{
    return mCertificates;
}

void ORMSQLite::dbOpened(const std::string &path, sqlite3 *const db)
{
    AutoLock lock(mMutex);
    db_map::iterator iter = mDbMap.find(path);
    if (iter == mDbMap.end())
    {
        //LOG_INFO(LOG_JNI, "[ORMSQLite::dbOpened] %s %p", path.c_str(), db);
        Sqlite *pDb = new Sqlite(path, db);
        mDbMap[path] = std::shared_ptr<Sqlite>(pDb);
    }
    else
    {
        //LOG_INFO(LOG_JNI, "[ORMSQLite::dbOpened] (re-open) %s %p", path.c_str(), db);
        Sqlite *pSqlite = iter->second.get();
        pSqlite->setJavaDb(db);
    }
}

void ORMSQLite::dbClosed(sqlite3 *const db)
{
    AutoLock lock(mMutex);
    for (db_map::iterator iter = mDbMap.begin(); iter != mDbMap.end();)
    {
        Sqlite *pSqlite = iter->second.get();
        if (pSqlite->isDbOwner(db))
        {
            //LOG_INFO(LOG_JNI, "[ORMSQLite::dbClosed] %s %p", pSqlite->getPath(), db);
            pSqlite->setJavaDb(nullptr);
            //iter = mDbMap.erase(iter);
            break;
        }
        else
        {
            iter++;
        }
    }
}

void ORMSQLite::dbClose(std::string path)
{
    AutoLock lock(mMutex);
    for (db_map::iterator iter = mDbMap.begin(); iter != mDbMap.end();)
    {
        if (std::string::npos != iter->first.find(path))
        {
            Sqlite *pSqlite = iter->second.get();
            pSqlite->setJavaDb(nullptr);
            iter = mDbMap.erase(iter);
        }
        else
        {
            iter++;
        }
    }
}

bool ORMSQLite::checkPassword()
{
    AutoSqlite sqlite(getDb('3'));

    if (sqlite.isOpen())
    {
        //LOG_INFO(LOG_JNI, "[ORMSQLite::dbOpened] open success");

        if (sqlite.get()->checkTables())
        {
            //LOG_INFO(LOG_JNI, "[ORMSQLite::dbOpened] check tables success");

            // reset attempts.
            //putSettingPending("COUNT", "", 0, 0, 0);

            return true;
        }
    }

    return false;
}

std::shared_ptr <Sqlite> ORMSQLite::getDb(char type)
{
    AutoLock lock(mMutex);
    for (db_map::iterator iter = mDbMap.begin(); iter != mDbMap.end(); ++iter)
    {
        const string &path = iter->first;

        // get the "extension" char (last char before any '?' args).
        char myType = path[path.length() - 4];
        size_t pos = path.find_first_of('?');
        if (string::npos != pos)
        {
            myType = path[pos - 4];
        }

        //LOG_INFO(LOG_JNI, "[ORMSQLite::getDb] check %c == %c", type, myType);
        if (myType == type)
        {
            //LOG_INFO(LOG_JNI, "[ORMSQLite::getDb] matched %s", iter->first.c_str());
            return iter->second;
        }
    }

    return nullptr;
}

std::shared_ptr <Sqlite> ORMSQLite::getDb(Schema *pSchema)
{
    if (nullptr != pSchema)
    {
        string schemaType = pSchema->getType();
        return getDb(schemaType[schemaType.length() - 1]);
    }

    return nullptr;
}

std::shared_ptr <Sqlite> ORMSQLite::getDb(Schema *pSchema, std::string findpath)
{
    AutoLock lock(mMutex);
    if (nullptr != pSchema)
    {
        string schemaType = pSchema->getType();
        char type = schemaType[schemaType.length() - 1];

        for (db_map::iterator iter = mDbMap.begin(); iter != mDbMap.end(); ++iter)
        {
            const string &path = iter->first;

            // get the "extension" char (last char before any '?' args).
            char myType = path[path.length() - 4];
            size_t pos = path.find_first_of('?');
            if (string::npos != pos)
            {
                myType = path[pos - 4];
            }

            //LOG_INFO(LOG_JNI, "[ORMSQLite::getDb] check %c == %c AND '%s' in '%s'", type, myType, findpath.c_str(), path.c_str());
            if (myType == type && path.find(findpath) != string::npos)
            {
                //LOG_INFO(LOG_JNI, "[ORMSQLite::getDb] matched %s", path.c_str());
                return iter->second;
            }
        }
    }

    return nullptr;
}

std::shared_ptr <Sqlite> ORMSQLite::getDb(std::string path, bool bCreate)
{
    AutoLock lock(mMutex);
    db_map::iterator iter = mDbMap.find(path);
    if (iter != mDbMap.end())
    {
        return iter->second;
    }
    else if (bCreate)
    {
        Sqlite *pDb = new Sqlite(path, nullptr);
        mDbMap[path] = std::shared_ptr<Sqlite>(pDb);
        return mDbMap[path];
    }
}

void ORMSQLite::checkpoint(char type)
{
    AutoSqlite sqlite(getDb(type));
    if (sqlite.isOpen())
    {
        sqlite.checkpoint();
    }
}

void ORMSQLite::addSchema(string tableStr, string colsStr, string typesStr, string anosStr,
                          string type)
{
    if (nullptr == getSchema(tableStr))
    {
        Schema *pSchema = new Schema(tableStr, colsStr, typesStr, anosStr, type);
        mSchemas[tableStr] = pSchema;

        //LOG_INFO(LOG_JNI, "[ORMSQLite::addSchema] [%s] %s", type.c_str(), tableStr.c_str());

        mMutex.lock();
        mQueue.push_back(NotifyQueueItem(NotifyType::NOTIFY_DBCHANGE, tableStr));
        mMutex.unlock();
    }
}

Schema *ORMSQLite::getSchema(string name)
{
    pschema_map::iterator iter = mSchemas.find(name);
    if (iter != mSchemas.end())
    {
        return iter->second;
    }

    return nullptr;
}

/*
* Store a java callback function in a callback map.
*/
void ORMSQLite::objectRegister(JNIEnv *env, const char *name, const char *method,
                               const char *signature, jobject obj)
{
    JCallBack *pCb = nullptr;

    mMutex.lock();

    pjcallback_map::iterator iter = mObjMap.find(name);
    if (iter != mObjMap.end())
    {
        pCb = iter->second;
    }
    else
    {
        jobject objref = (jobject) env->NewGlobalRef(obj);
        jclass tmp = env->GetObjectClass(objref);
        jclass cls = (jclass) env->NewGlobalRef(tmp);

        jmethodID mid = env->GetMethodID(cls, method, signature);

        pCb = new JCallBack(objref, cls);
        mObjMap[name] = pCb;
    }

    mMutex.unlock();

    if (nullptr != pCb)
    {
        method_map::iterator iter = pCb->mMethods.find(method);
        if (iter == pCb->mMethods.end())
        {
            jmethodID mid = env->GetMethodID(pCb->mClass, method, signature);
            pCb->mMethods[method] = mid;
        }
    }
}

/*
* Store a java callback function in a callback map.
*/
JCallBack *ORMSQLite::objectFind(const char *name)
{
    pjcallback_map::iterator iter = mObjMap.find(name);
    if (iter != mObjMap.end())
    {
        return iter->second;
    }
    return nullptr;
}

/**
 * Clears all references to Java methods stored in memory.
 */
void ORMSQLite::objectClear()
{
    mMutex.lock();

    mObjMap.clear();

    mMutex.unlock();
}

bool ORMSQLite::checkTries()
{
    string textVal;
    uint64_t longVal = 0;
    uint64_t longEnabled = 0;
    uint64_t timeStamp = 0;

    getSetting("COUNT", textVal, longVal, longEnabled, timeStamp);

    bool bResult = (longVal < PIN_MAX_TRIES);

    //LOG_INFO(LOG_JNI, "[ORMSQLite::checkTries] [%s] %" PRIu64, (bResult ? "true" : "false"), longVal);

    longVal++;
    putSettingPending("COUNT", textVal, longVal, longEnabled, timeStamp);

    if (longVal > 6)
    {
        int seconds = (longVal * longVal) / 2;
        //LOG_INFO(LOG_JNI, "[ORMSQLite::checkTries] sleep %d", seconds);
        sleep(seconds);
    }

    //if (!bResult)
    //    callLoginRequired(FLAG_LOGIN_PINCOUNT);

    return bResult;
}

void ORMSQLite::setPassword(const char *database, const char *password)
{
    if (Utils::isDebuggerConnected())
        return;

    Sqlite *pDb = getDb(database, true).get();
    if (nullptr != pDb)
    {
        string dest;
        string src = password;

        Utils::encodeBase91(dest, src);
        pDb->setPassword(dest.c_str());
    }
}

void ORMSQLite::changePassword(const char *password)
{
    if (Utils::isDebuggerConnected())
        return;

    Sqlite *pDb = getDb('3').get();
    if (nullptr != pDb)
    {
        string dest;
        string src = password;
        Utils::encodeBase91(dest, src);
        pDb->changePassword(dest.c_str());
    }
}

void ORMSQLite::pushExecPending(char dbNo, const string &data)
{
    // get the 'user secure' database
    //LOG_INFO(LOG_JNI, "[ORMSQLite::pushExecPending] [%c] %s", dbNo, data.c_str());
    //Utils::trace("[ORMSQLite::pushExecPending]", data.c_str());

    db_pending_map::iterator it = mDbPendingMap.find(dbNo);
    if (it == mDbPendingMap.end())
    {
        //LOG_INFO(LOG_JNI, "[ORMSQLite::pushExecPending] new list");

        // create new vector.
        mDbPendingMap[dbNo] = vector<string>();
    }

    mDbPendingMap[dbNo].push_back(data);
}

void ORMSQLite::execPending()
{
    doExecPending();
}

void ORMSQLite::doExecPending()
{
    AutoLock lock(mMutex);

    if (!mPendingStarted)
    {
        mPendingStarted = true;

        //LOG_INFO(LOG_JNI, "[ORMSQLite::doExecPending]");

        for (db_pending_map::iterator it = mDbPendingMap.begin(); it != mDbPendingMap.end(); it++)
        {
            //LOG_INFO(LOG_JNI, "[ORMSQLite::doExecPending] try open db:%c", it->first);

            AutoSqlite sqlite(getDb(it->first));
            if (sqlite.isOpen())
            {
                //LOG_INFO(LOG_JNI, "[ORMSQLite::doExecPending] db:%c count:%d", it->first,
                //         it->second.size());
                
                // execute any pending transactions...
                //TODO: handle for each database type.
                for (vector<string>::iterator iter = it->second.begin();
                     iter != it->second.end();)
                {
                    string &exec = *iter;
                    //LOG_INFO(LOG_JNI, "[ORMSQLite::doExecPending] exec: %s", exec.c_str());
                    if (sqlite.exec(exec.c_str()))
                    {
                        iter = it->second.erase(iter);
                    }
                    else
                    {
                        iter++;
                    }
                }
            }
        }

        mPendingStarted = false;
    }
}

void ORMSQLite::callLoginRequired(int flags)
{
    JCallBack *pCb;
    pCb = ORMSQLite::getInstance()->objectFind("loginRequired");
    if (nullptr != pCb)
    {
        AutoJniEnv env;
        env->CallVoidMethod(pCb->mObject, pCb->getMethod("loginRequired"), flags);
    }
}

void ORMSQLite::callTokenRefreshed()
{
    JCallBack *pCb;
    pCb = ORMSQLite::getInstance()->objectFind("tokenRefreshed");
    if (nullptr != pCb)
    {
        AutoJniEnv env;
        env->CallVoidMethod(pCb->mObject, pCb->getMethod("tokenRefreshed"));
    }
}

string ORMSQLite::getUser()
{
    if (mUsername.length() <= 0)
    {
        getSetting("USERNAME0", mUsername);
    }

    return mUsername;
}

void ORMSQLite::setAccessToken(int apiIx, field_map& data)
{
    // set model id.
    data["id"] = Field(getApiId(apiIx, false), FieldType::STRING, true);

    if (data.end() != data.find("username"))
    {
        mUsername = data["username"].getRaw();
        putSettingStr(getApiSettingName(apiIx, "USERNAME").c_str(), mUsername);
        //LOG_INFO(LOG_JNI, "[ORMSQLite::setAccessToken] username %s", mUsername.c_str());
    }
}

std::string ORMSQLite::getToken(int apiIx)
{
    string token = "";

    getSetting("ID_TOKEN", token);
    return token;
}

std::string ORMSQLite::getEnvironment(int apiIx)
{
    return getToken_env();
}

std::string ORMSQLite::getAppId(int apiIx)
{
    return getToken_aid();
}

std::string ORMSQLite::getVendorId(int apiIx)
{
    return getToken_vid();
}

std::string ORMSQLite::getSmallbatch(int apiIx)
{
    Schema *pSchema = getSchema("ModelAppConf");
    if (nullptr != pSchema)
    {
        AutoSqlite sqlite(getDb(pSchema));
        if (sqlite.isOpen())
        {
            std::string value = sqlite.getValue("ModelAppConf", "submitbatch", nullptr);
            //std::cout << "[ORMSQLite::getSmallbatch] " << value << std::endl;
            return value;
        }
    }

    return "";
}

std::string ORMSQLite::getBinaryKeys()
{
    Schema *pSchema = getSchema("ModelAppConf");
    if (nullptr != pSchema)
    {
        AutoSqlite sqlite(getDb(pSchema));
        if (sqlite.isOpen())
        {
            std::string value = sqlite.getValue("ModelAppConf", "binary_keys", nullptr);
            std::string encoded;
            Utils::decodeBase91(encoded, value, Format::B91STRING);
            //std::cout << "[ORMSQLite::getBinaryKeys] " << encoded << std::endl;
            return encoded;
        }
    }

    return "";
}

std::string ORMSQLite::getInterfaceUrl(int apiIx)
{
    std::string value = "";
    getKey("DEBUG_HARNESS", value);
    if (value.length() > 0)
        return value;
    Schema *pSchema = getSchema("ModelAppConf");
    if (nullptr != pSchema)
    {
        AutoSqlite sqlite(getDb(pSchema));
        if (sqlite.isOpen())
        {
            value = sqlite.getValue("ModelAppConf", "interface_url", nullptr);
            //std::cout << "[ORMSQLite::getInterfaceUrl] " << value << std::endl;
            return value;
        }
    }

    return "";
}

std::string ORMSQLite::getResourceUrl(int apiIx)
{
    std::string value = "";
    getKey("DEBUG_RESOURCE_SVR", value);
    if (value.length() > 0)
        return value;

    /* TODO
    Schema *pSchema = getSchema("ModelAppConf");
    if (nullptr != pSchema)
    {
        AutoSqlite sqlite(getDb(pSchema));
        if (sqlite.isOpen())
        {
            value = sqlite.getValue("ModelAppConf", "interface_url", nullptr);
            std::cout << "[ORMSQLite::getInterfaceUrl] " << value << std::endl;
            return value;
        }
    }
    */

    return "";
}

string ORMSQLite::getLastResponse(const char *endPoint)
{
    //LOG_INFO(LOG_JNI, "[ORMSQLite::getLastResponse] %s", endPoint);

    // Get endPoint minus any query parameters (if present).
    std::string baseEndpoint = endPoint;
    baseEndpoint = baseEndpoint.substr(0, baseEndpoint.find('?'));

    std::string result;
    if (getEtag(baseEndpoint, result))
    {
        std::ostringstream resultStrm;
        resultStrm << '"' << result << '"';
        return resultStrm.str();
    }

    return "";
}

void ORMSQLite::setLastResponse(const char *endPoint, const char *time, std::string headers)
{
    //LOG_INFO(LOG_JNI, "[ORMSQLite::setLastResponse] %s", endPoint);

    std::regex etag_marker_regex("[Ee][Tt][Aa][Gg]\\s*:", std::regex::extended|std::regex::icase);
    if (std::regex_search(headers, etag_marker_regex))
    {
        // Get endPoint minus any query parameters (if present).
        std::string baseEndpoint = endPoint;
        baseEndpoint = baseEndpoint.substr(0, baseEndpoint.find('?'));

        /*
        // This works on linux but for some reason doesn't on Android...
        std::regex etag_regex(".*[Ee][Tt][Aa][Gg]\\s*:[^\"]*\"([^\"]*)\".*");
        std::string etag = std::regex_replace(headers, etag_regex, "$1");
        */

        std::regex etag_regex("[Ee][Tt][Aa][Gg]\\s*:[^\"]*\"[^\"]*\"");
        auto words_begin = std::sregex_iterator(headers.begin(), headers.end(), etag_regex);
        auto words_end = std::sregex_iterator();
        for (std::sregex_iterator i = words_begin; i != words_end; ++i)
        {
            std::smatch match = *i;
            std::string match_str = match.str();
            std::string substr = match_str.substr(match_str.find('"'), match_str.rfind('"'));
            if ('"' == substr.at(0))
            {
                substr.erase(0, 1);
            }

            size_t len = substr.length();
            if ('"' == substr.at(len - 1))
            {
                substr.erase(len - 1, 1);
            }

            setEtag(baseEndpoint, substr);
            break;
        }
    }
}

void ORMSQLite::initKeyStore()
{
    // for persistence of keys etc - used by get/set Key.
    addSchema("store", "pk,id,name,value", "long,string,string,string", "appDb=PRIMARY KEY AUTOINCREMENT:idMap=0,idMap=0,idMap=0,idMap=0", ".");
    Schema *pSchema = getSchema("store");
    if (nullptr != pSchema)
    {
        AutoSqlite sqlite(getDb(pSchema));
        if (sqlite.isOpen())
        {
            //LOG_INFO(LOG_SQLITE, "[ORMSQLite::initKeyStore] about to create table");
            pSchema->createTable(&sqlite);
        }
    }
}

bool ORMSQLite::getKey(std::string name, string& val)
{
    //LOG_INFO(LOG_JNI, "[ORMSQLite::getKey] %s", name.c_str());
    std::string id = getToken_uid() + name;

    Schema *pSchema = getSchema("store");
    if (nullptr != pSchema)
    {
        AutoSqlite sqlite(getDb(pSchema));
        if (sqlite.isOpen())
        {
            field_map data;
            ostringstream where;

            std::string id = getToken_uid() + name;

            where << "id" << "='" << id << "'";

            string whereStr = where.str();
            sqlite.getValues(pSchema, data, whereStr.c_str());
            if (data.find("value") != data.end())
            {
                val = data["value"].getRaw();
                return true;
            }
        }
    }

    return false;
}

void ORMSQLite::setKey(std::string name, string val)
{
    //LOG_INFO(LOG_JNI, "[ORMSQLite::setKey] %s", name.c_str());
    std::string id = getToken_uid() + name;

    Schema *pSchema = getSchema("store");
    if (nullptr != pSchema)
    {
        field_map data;
        pSchema->putValue(data, "id", id.c_str());
        pSchema->putValue(data, "name", name.c_str());
        pSchema->putValue(data, "value", val.c_str());
        pSchema->store(nullptr, data);
    }
}

void ORMSQLite::initEtagStore()
{
    // for persistence of ETag(s) etc - used by get/set ETag.
    addSchema("etags", "pk,id,name,value", "long,string,string,string", "appDb=PRIMARY KEY AUTOINCREMENT:idMap=0,idMap=0,idMap=0,idMap=0", ".");
    Schema *pSchema = getSchema("etags");
    if (nullptr != pSchema)
    {
        AutoSqlite sqlite(getDb(pSchema));
        if (sqlite.isOpen())
        {
            //LOG_INFO(LOG_SQLITE, "[ORMSQLite::initEtagStore] about to create table");
            pSchema->createTable(&sqlite);
        }
    }
}

bool ORMSQLite::getEtag(std::string name, string& val)
{
    std::string id = getToken_uid() + name;

    Schema *pSchema = getSchema("etags");
    if (nullptr != pSchema)
    {
        AutoSqlite sqlite(getDb(pSchema));
        if (sqlite.isOpen())
        {
            field_map data;
            ostringstream where;

            std::string id = getToken_uid() + name;

            where << "id" << "='" << id << "'";

            string whereStr = where.str();
            sqlite.getValues(pSchema, data, whereStr.c_str());
            if (data.find("value") != data.end())
            {
                val = data["value"].getRaw();
                LOG_INFO(LOG_JNI, "[ORMSQLite::getEtag] %s %s", name.c_str(), val.c_str());
                return true;
            }
        }
    }

    return false;
}

void ORMSQLite::setEtag(std::string name, string val)
{
    LOG_INFO(LOG_JNI, "[ORMSQLite::setEtag] %s : %s", name.c_str(), val.c_str());
    std::string id = getToken_uid() + name;

    Schema *pSchema = getSchema("etags");
    if (nullptr != pSchema)
    {
        field_map data;
        pSchema->putValue(data, "id", id.c_str());
        pSchema->putValue(data, "name", name.c_str());
        pSchema->putValue(data, "value", val.c_str());
        pSchema->store(nullptr, data);
    }
}

void ORMSQLite::putSettingStr(const char *name, string textVal)
{
    setKey(name, textVal);
}

void ORMSQLite::putSetting(const char *name, string textVal, uint64_t longVal, uint64_t longEnabled,
                           uint64_t timeStamp)
{
    putSettingPending(name, textVal, longVal, longEnabled, timeStamp, false);
}

void ORMSQLite::putSettingPending(const char *name, string textVal, uint64_t longVal,
                                  uint64_t longEnabled, uint64_t timeStamp, bool bPending)
{
    // get the Settings schema.
    Schema *pSchema = getSchema("ModelSetting");
    if (nullptr != pSchema)
    {
        field_map data;
        pSchema->putValue(data, "name", name);
        pSchema->putValue(data, "textValue", textVal.c_str());
        pSchema->putValue(data, "longValue", longVal);
        pSchema->putValue(data, "enabled", longEnabled);
        pSchema->putValue(data, "timeStamp", timeStamp);
        pSchema->putValue(data, "deleted", (int64_t) 0);
        pSchema->printValues("[ORMSQLite::putSettingPending]", data);
        pSchema->store(nullptr, data);
    }
}

void ORMSQLite::getSetting(const char *name, string &textVal, uint64_t &longVal,
                           uint64_t &longEnabled, uint64_t &timeStamp)
{
    Schema *pSchema = getSchema("ModelSetting");
    if (nullptr != pSchema)
    {
        // get the global db.
        AutoSqlite sqlite(getDb(pSchema));
        if (sqlite.isOpen())
        {
            field_map data;
            ostringstream where;

            where << "name" << "='" << name << "'";

            //LOG_INFO(LOG_JNI, "[ORMSQLite::getSetting] get values");

            string whereStr = where.str();
            sqlite.getValues(pSchema, data, whereStr.c_str());

            //LOG_INFO(LOG_JNI, "[ORMSQLite::getSetting] got values");
            //pSchema->printValues("[ORMSQLite::getSetting]", data);

            textVal = data["textValue"].getRaw();
            longVal = data["longValue"].getUint64();
            longEnabled = data["enabled"].getUint64();
            timeStamp = data["timeStamp"].getUint64();
        }
    }
}

bool ORMSQLite::getSetting(const char *name, string &textVal)
{
    return getKey(name, textVal);
}

string ORMSQLite::getUrl(const char *endPoint, int flags)
{
    std::ostringstream tempStrm;
    if (flags & FLAG_NOBASE)
    {
        tempStrm << endPoint;
    }
    else
    {
        tempStrm << mBaseUrl << endPoint;
    }

    if (flags & FLAG_SIGN_URL)
    {
        RsaUtils rsa(STR_key_url, false);

        // Generate the access policy
        std::ostringstream resultStrm;

        auto expiry = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch() + std::chrono::hours(1)).count();

        resultStrm << "{\"Statement\":[{\"Resource\":\"" << tempStrm.str() << "\",\"Condition\":{\"DateLessThan\":{\"AWS:EpochTime\":" << expiry << "}}}]}";
        std::string result = resultStrm.str();
        //std::cout << "[ORMSQLite::getUrl] JSON:" << result << std::endl;

        // Sign the policy
        std::vector<unsigned char> signature;
        rsa.rsaSign((const unsigned char*)result.c_str(), result.length(), signature);
        std::string base64Signature = Base64::getInstance()->base64_encode(signature.data(), signature.size(), false, true);

        // AWS doesn't use standard base64 URL safe encoding, so use their custom format
        std::replace(base64Signature.begin(), base64Signature.end(), '+', '-');
        std::replace(base64Signature.begin(), base64Signature.end(), '=', '_');
        std::replace(base64Signature.begin(), base64Signature.end(), '/', '~');

        //RsaUtils::base64Encode(signature.data(), signature.size(), base64Signature);

        //?Expires={}&Signature={}&Key-Pair-Id={}
        tempStrm << "?Expires=" << expiry << "&Signature=" << base64Signature << "&Key-Pair-Id=" << Utils::decodeStr(STR_key_url_id);

        //std::cout << "[ORMSQLite::getUrl]:" << tempStrm.str() << std::endl;
    }

    return tempStrm.str();
}

bool ORMSQLite::isNetworkAvailable()
{
    bool bResult = false;

    // get the UUID from java...
    JCallBack *pCb = ORMSQLite::getInstance()->objectFind("isNetworkAvailable");
    if (nullptr != pCb)
    {
        AutoJniEnv env;
        bResult = env->CallBooleanMethod(pCb->mObject, pCb->getMethod("isNetworkAvailable"));
    }

    return bResult;
}

string ORMSQLite::stripGuard(string &src)
{
    std::regex guard_regex("\n?-+BEGIN[A-Z ]+-+\n?([A-Za-z0-9/+=\n]*)\n?-+END[A-Z ]+-+\n?",
                           std::regex_constants::extended);
    std::regex newline_regex("(.*)\n+",
                             std::regex_constants::extended);

    string result = std::regex_replace(src, guard_regex, "$1");
    result = std::regex_replace(result, newline_regex, "$1");
    Utils::traceErr("[ORMSQLite::stripGuard]\n", result.c_str());
    return result;
}

bool ORMSQLite::hasCertKeys(string &pubKey, string &priKey)
{
    bool bResult = false;

    Schema *pSchema = getSchema("ModelSettingsSecure");
    if (nullptr != pSchema)
    {
        // get the ModelSettingsSecure db.
        AutoSqlite sqlite(getDb(pSchema));
        if (sqlite.isOpen())
        {
            field_map pubData, priData;
            string pubWhereStr = "name='PUBLIC_KEY'";
            string priWhereStr = "name='PRIVATE_KEY'";

            sqlite.getValues(pSchema, pubData, pubWhereStr.c_str());
            sqlite.getValues(pSchema, priData, priWhereStr.c_str());

            if ((pubData.end() != pubData.find("value")) &&
                (priData.end() != priData.find("value")))
            {
                pubKey = pubData["value"].getRaw();
                priKey = priData["value"].getRaw();
                bResult = true;
            }
        }
    }

    return bResult;
}

string ORMSQLite::readCertificate(const char *certKey)
{
    Schema *pSchema = getSchema("ModelSettingsSecure");
    if (nullptr != pSchema)
    {
        // get the ModelSettingsSecure db.
        AutoSqlite sqlite(getDb(pSchema));
        if (sqlite.isOpen())
        {
            field_map data;
            std::ostringstream whereStm;
            string whereStr;

            whereStm << "name='" << certKey << "'";
            whereStr = whereStm.str();
            sqlite.getValues(pSchema, data, whereStr.c_str());

            if (data.end() != data.find("value"))
            {
                return data["value"].getRaw();
            }
        }
    }

    return "";
}

bool ORMSQLite::hasCertificate(const char *savePoint, const char *expSavePoint)
{
    bool bResult = false;

    Schema *pSchema = getSchema("ModelSettingsSecure");
    if (nullptr != pSchema)
    {
        // get the ModelSettingsSecure db.
        AutoSqlite sqlite(getDb(pSchema));
        if (sqlite.isOpen())
        {
            field_map data;
            ostringstream whereStrm;

            whereStrm << "name='" << savePoint << "'";
            string whereStr = whereStrm.str();

            sqlite.getValues(pSchema, data, whereStr.c_str());

            if (data.end() != data.find("value"))
            {
                string certExpiry;
                getSetting(expSavePoint, certExpiry);
                if (!Utils::checkTimeExpired(certExpiry))
                    bResult = true;
            }
        }
    }

    return bResult;
}

void ORMSQLite::handleCertResponse(int responseCode, int &result, string &response,
                                   const char *savePoint, const char *expSavePoint)
{
    switch (responseCode)
    {
        case 200:
        {
            Schema *pSchema = getSchema("ModelSettingsSecure");
            if (nullptr != pSchema)
            {
                Value root;
                Json::Reader reader;

                reader.parse(response, root, false);
                if (root.isMember("response"))
                {
                    field_map data;
                    string cert = root["response"].asString();
                    pSchema->putValue(data, "name", savePoint);
                    pSchema->putValue(data, "value", cert.c_str());
                    pSchema->store(nullptr, data);

                    ptime t1 = OpenSslUtils::get_expiry(cert.c_str());
                    putSettingStr(expSavePoint, Utils::getTimeStr(t1));
                    result = 1;
                }
            }
        }
            break;

        case 304:
            result = 0;
            break;

        default:
            result = -3;
            break;
    }
}

//TODO: check if the certificate has expired & renew if required.
int ORMSQLite::createCertificate()
{
    vector<const char *> empty_args;
    Api &api = getApi();
    string pubKey;
    string priKey;
    string csr;
    string response;
    bool bHasKeys = hasCertKeys(pubKey, priKey);
    bool bHasCert = hasCertificate("CERTIFICATE", "CERT_EXPIRY");
    int responseCode = 200;
    int result = 0;
    bool bMore;

    if (!bHasKeys)
    {
        // generate keys -> cert needs re-creation.
        bHasCert = false;
        bHasKeys = OpenSslUtils::generate_keys(priKey, pubKey);
        if (bHasKeys)
        {
            //Utils::traceErr("[ORMSQLite::createCertificate] private key", priKey.c_str());
            //Utils::traceErr("[ORMSQLite::createCertificate] public key", pubKey.c_str());

            Schema *pSchema = getSchema("ModelSettingsSecure");
            if (nullptr != pSchema)
            {
                field_map data;

                pSchema->putValue(data, "name", "PUBLIC_KEY");
                pSchema->putValue(data, "value", pubKey.c_str());
                pSchema->store(nullptr, data);

                pSchema->putValue(data, "name", "PRIVATE_KEY");
                pSchema->putValue(data, "value", priKey.c_str());
                pSchema->store(nullptr, data);
            }
        }
    }

    if (!bHasCert)
        result = -1;

    if (!bHasKeys)
        result = -2;

    if (!hasCertificate("ROOTCERT", "EXP_ROOTCERT"))
    {
        //TODO: check apiIx...
        response = api.apiGet(0, "", "pki/rootcert", responseCode, bMore, 0, FLAG_RESPONSE,
                              empty_args);
        handleCertResponse(responseCode, result, response, "ROOTCERT", "EXP_ROOTCERT");
    }

    //TODO: enable when API is in UAT.
//    if (!hasCertificate("DEVCACERT", "EXP_DEVCACERT"))
//    {
//        response = api.apiGet(0, "", "pki/devicecacert", responseCode, bMore, 0, FLAG_RESPONSE, empty_args);
//        handleCertResponse(responseCode, result, response, "DEVCACERT", "EXP_DEVCACERT");
//    }

    if (!hasCertificate("SUBCACERT", "EXP_SUBCACERT"))
    {
        response = api.apiGet(0, "", "pki/subcacert", responseCode, bMore, 0, FLAG_RESPONSE,
                              empty_args);
        handleCertResponse(responseCode, result, response, "SUBCACERT", "EXP_SUBCACERT");
    }

    if (bHasKeys && !bHasCert)
    {
        //LOG_INFO(LOG_JNI, "[ORMSQLite::createCertificate]");

        if (OpenSslUtils::generate_csr("MYQ", priKey, pubKey, csr))
        {
            ostringstream postDataStrm;
            string postData;
            vector<const char *> args;
            RSA *rsaPriKey = nullptr;

            //Utils::traceErr("[ORMSQLite::createCertificate] csr", csr.c_str());

            Value jwt_header;
            Value jwt_payload;

            jwt_header["alg"] = "RS256";
            jwt_header["typ"] = "JWT";

            jwt_payload["certificate"] = pubKey;
            jwt_payload["csr"] = stripGuard(csr);

            string jws = Jws::encodeJws(jwt_header, jwt_payload, priKey);

            //Utils::traceErr("[ORMSQLite::createCertificate] jws", jws.c_str());

            postDataStrm << "{\"token\": \"" << jws << "\"}";
            postData = postDataStrm.str();
            args.push_back(postData.c_str());
            //TODO: check apiIx
            response = api.apiPost(0, "", "pki/clientcert", responseCode,
                                   FLAG_DATA | FLAG_RESPONSE, args);

            handleCertResponse(responseCode, result, response, "CERTIFICATE", "CERT_EXPIRY");
        }
    }

    return result;
}

string ORMSQLite::getDeviceId()
{
    return mDeviceId;
}

void ORMSQLite::run(void *pthis)
{
    while (mRun)
    {
        mMutex.lock();
        std::vector<NotifyQueueItem>::iterator iter = mQueue.begin();

        while (iter != mQueue.end() && mRun)
        {
            if (!(*iter).notify(mMutex))
            {
                // no more notify items expected -> remove from queue.
                iter = mQueue.erase(iter);
            }
            else
            {
                iter++;
            }
        }

        mMutex.unlock();

        if (mRun)
            mCondition.wait(NOTIFY_PERIOD/4);
    }
}

std::string ORMSQLite::getAsset(std::string name)
{
    std::string result = "";
    JCallBack *pCb = objectFind("getAsset");
    if (nullptr != pCb)
    {
        AutoJniEnv env;
        AutoJniFrame(env, 1);
        jstring jNameStr = env->NewStringUTF(name.c_str());
        jstring jFilename = (jstring)env->CallObjectMethod(pCb->mObject, pCb->getMethod("getAsset"), jNameStr);
        env->DeleteLocalRef(jNameStr);

        if (nullptr != jFilename)
        {
            const char *strFilename = env->GetStringUTFChars(jFilename, 0);
            result = strFilename;
            env->ReleaseStringUTFChars(jFilename, strFilename);
            env->DeleteLocalRef(jFilename);
        }
    }

    return result;
}

std::string ORMSQLite::getAssetIfReady(std::string name)
{
    std::string result = "";
    JCallBack *pCb = objectFind("getAssetIfReady");
    if (nullptr != pCb)
    {
        AutoJniEnv env;
        AutoJniFrame(env, 1);
        jstring jNameStr = env->NewStringUTF(name.c_str());
        jstring jFilename = (jstring)env->CallObjectMethod(pCb->mObject, pCb->getMethod("getAssetIfReady"), jNameStr);
        env->DeleteLocalRef(jNameStr);

        if (nullptr != jFilename)
        {
            const char *strFilename = env->GetStringUTFChars(jFilename, 0);
            result = strFilename;
            env->ReleaseStringUTFChars(jFilename, strFilename);
            env->DeleteLocalRef(jFilename);
        }
    }

    return result;
}

std::string ORMSQLite::getUuid()
{
    std::string result = "";
    JCallBack *pCb = objectFind("getUuid");
    if (nullptr != pCb)
    {
        AutoJniEnv env;
        AutoJniFrame(env, 1);
        jstring jUuid = (jstring)env->CallObjectMethod(pCb->mObject, pCb->getMethod("getUuid"));

        if (nullptr != jUuid)
        {
            const char *strUuid = env->GetStringUTFChars(jUuid, 0);
            result = strUuid;
            env->ReleaseStringUTFChars(jUuid, strUuid);
            env->DeleteLocalRef(jUuid);
        }
    }

    return result;
}

std::string ORMSQLite::getUserId()
{
    std::string token;
    getKey("ID_TOKEN", token);
    if (!token.empty())
    {
        std::vector<std::string> elements;
        Utils::split(token, '.', elements);
        if (elements.size()>2)
        {
            Json::Reader reader;
            Json::Value json;
            Base64 *b64 = Base64::getInstance();
            std::string jsonStr = b64->base64_decode_toStr(elements[1]);
            if (reader.parse(jsonStr, json, false))
            {
                if (json.isMember("cognito:username"))
                {
                    token = json["cognito:username"].asString();
                }
            }
        }
    }

    return token;
}

std::string ORMSQLite::getUserIdHash()
{
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256_CTX sha256;
    int rc = 1;

    Base64 *b64 = Base64::getInstance();
    std::string userId = getUserId();

    if (rc > 0)
        rc = SHA256_Init(&sha256);

    if (rc > 0)
        rc = SHA256_Update(&sha256, userId.c_str(), userId.length());

    if (rc > 0)
        rc = SHA256_Final(hash, &sha256);

    return b64->base64_encode(hash, SHA256_DIGEST_LENGTH);
}


void ORMSQLite::uploadPayload(std::string name)
{
    JCallBack *pCb = objectFind("uploadPayload");
    if (nullptr != pCb)
    {
        AutoJniEnv env;
        AutoJniFrame(env, 1);
        jstring jNameStr = env->NewStringUTF(name.c_str());
        env->CallVoidMethod(pCb->mObject, pCb->getMethod("uploadPayload"), jNameStr);
        env->DeleteLocalRef(jNameStr);
    }
}

void ORMSQLite::doNotifyTableChanged(const string &table)
{
    //LOG_INFO(LOG_JNI, "doNotifyTableChanged:%s", table->c_str());
    JCallBack *pCb = objectFind("notifyChangeFromNative");
    if (nullptr != pCb)
    {
        AutoJniEnv env;
        AutoJniFrame(env, 1);
        jstring jTableStr = env->NewStringUTF(table.c_str());
        env->CallVoidMethod(pCb->mObject, pCb->getMethod("notifyChangeFromNative"), jTableStr);
        env->DeleteLocalRef(jTableStr);
    }
}

void ORMSQLite::queueNotifyTableChanged(const char *table, const char* id)
{
    mMutex.lock();
    std::vector<NotifyQueueItem>::iterator iter = mQueue.begin();

    while (iter != mQueue.end())
    {
        if ((*iter).type() == NotifyType::NOTIFY_DBCHANGE && (*iter).table() == table)
        {
            (*iter).pending(mMutex, id);
            mCondition.signal();
            break;
        }
        iter++;
    }

    mMutex.unlock();
}

void ORMSQLite::notifyTableChanged(const char *table, const char* id)
{
    //LOG_INFO(LOG_JNI, "[ORMSQLite::notifyTableChanged] [%s]", table);
    queueNotifyTableChanged(table, id);
}

void ORMSQLite::apiProgressCallback(std::string table,
                                    curl_off_t dltotal,
                                    curl_off_t dlnow,
                                    curl_off_t ultotal,
                                    curl_off_t ulnow)
{
    std::vector<int64_t> values;
    values.push_back(dltotal);
    values.push_back(dlnow);
    values.push_back(ultotal);
    values.push_back(ulnow);

    mMutex.lock();
    bool bFound = false;
    std::vector<NotifyQueueItem>::iterator iter = mQueue.begin();

    while (iter != mQueue.end())
    {
        if ((*iter).type() == NotifyType::NOTIFY_API_PROGRESS && (*iter).table() == table)
        {
            //LOG_INFO(LOG_GLOBAL, "apiProgressCallback (found) %s %d %d %d %d", table.c_str(), dltotal, dlnow, ultotal, ulnow);
            (*iter).updateInt(mMutex, values);
            mCondition.signal();
            bFound = true;
            break;
        }
        iter++;
    }

    if (!bFound)
    {
        //LOG_INFO(LOG_GLOBAL, "apiProgressCallback (new) %s %d %d %d %d", table.c_str(), dltotal, dlnow, ultotal, ulnow);
        NotifyQueueItem item(NotifyType::NOTIFY_API_PROGRESS, table);
        item.updateInt(mMutex, values);
        mQueue.push_back(item);
        mCondition.signal();
    }

    mMutex.unlock();
}

void ORMSQLite::setFlag(const char* flag, const char* value)
{
    mFlags[flag] = value;

//    if (0 == strcmp(flag, "visualize"))
//    {
//        if (getFlagBool("visualize"))
//        {
//            FactoryTensor::getInstance()->registerVisualizeCallback(visualize_cb);
//        }
//        else
//        {
//            FactoryTensor::getInstance()->registerVisualizeCallback(nullptr);
//        }
//    }
}

bool ORMSQLite::hasFlag(const char* flag)
{
    string_map::iterator iter = mFlags.find(flag);
    return (iter != mFlags.end());
}

std::string ORMSQLite::getFlag(const char* flag)
{
    string_map::iterator iter = mFlags.find(flag);
    if (iter != mFlags.end())
    {
        return iter->second;
    }

    return "";
}

bool ORMSQLite::getFlagBool(const char* flag, bool bDefault)
{
    std::string value = getFlag(flag);
    if (value.length() > 0)
    {
        bool b;
        std::transform(value.begin(), value.end(), value.begin(), ::tolower);
        std::istringstream is(value);
        is >> std::boolalpha >> b;
        return b;
    }

    return bDefault;
}

int ORMSQLite::getPoseFrameCnt()
{
    // Defined in Constants.hpp
    int result = POSE_FRAME_COUNT;

    std::string setting;
    getSetting("DEBUG_POSEFRAMES", setting);
    if (!setting.empty())
    {
        result = std::stoi(setting);
    }

    return result;
}

bool ORMSQLite::hasTokens()
{
    return (
            (mToken_aid.length() > 0) &&
            (mToken_cid.length() > 0) &&
            (mToken_env.length() > 0) &&
            (mToken_vid.length() > 0)
            );
}

void ORMSQLite::setEnv(std::string aid, std::string cid, std::string created, std::string env, std::string vid)
{
    mToken_aid = aid;
    mToken_cid = cid;
    mToken_created = created;
    mToken_env = env;
    mToken_vid = vid;
}

void ORMSQLite::setEnv(vector<std::string>& token)
{
    mToken_aid = token[0];
    mToken_cid = token[1];
    mToken_created = token[2];
    mToken_env = token[3];
    mToken_vid = token[4];
}

std::string ORMSQLite::getToken_aid()
{
    return mToken_aid;
}

std::string ORMSQLite::getToken_cid()
{
    return mToken_cid;
}

std::string ORMSQLite::getToken_created()
{
    return mToken_created;
}

std::string ORMSQLite::getToken_env()
{
    return mToken_env;
}

std::string ORMSQLite::getToken_vid()
{
    return mToken_vid;
}

std::string ORMSQLite::getToken_uid()
{
    return mToken_aid+mToken_cid+mToken_vid+mToken_env;
}

void ORMSQLite::setSecret(std::string secret)
{
    mSecret = secret;
}

std::string ORMSQLite::getSecret()
{
    return mSecret;
}

void ORMSQLite::setState(char state)
{
    mState = state;
}

char ORMSQLite::getState()
{
    return mState;
}

bool ORMSQLite::stateCapture()
{
    // Get the saved 'capture' state if it's been set...
    // default to mState value.
    // If 'capture' has been saved it will override the 'mState' value.
    std::string keyState(1, mState);
    getKey("capture", keyState);

    if (keyState.length() > 0)
    {
        switch (keyState.at(0))
        {
            case 's':
            case 'S':
                return true;

            // numeric number of payg captures?
            case '1' ... '9':
                return true;
        }
    }

    return false;
}

void ORMSQLite::setGuest(const char *guestUsername)
{
    mGuestUsername = guestUsername;
}

string ORMSQLite::getGuest()
{
    return mGuestUsername;
}

void ORMSQLite::initBilling(const char* url, const char* key)
{
    mBilling = new Billing(url, key);
}

bool ORMSQLite::billing_sync(bool bAsync)
{
    if (nullptr != mBilling)
    {
        return mBilling->sync(bAsync);
    }

    return false;
}

bool ORMSQLite::billing_eventPending(std::string field, std::string value)
{
    if (nullptr != mBilling)
    {
        return mBilling->eventPending(field, value);
    }

    return false;
}

bool ORMSQLite::billing_logEvent(BillingEventId eventId, std::string attemptId, Json::Value eventMisc)
{
    if (nullptr != mBilling)
    {
        return mBilling->logEvent(eventId, attemptId, eventMisc);
    }

    return false;
}

bool ORMSQLite::billing_logEventSync(BillingEventId eventId, std::string attemptId, Json::Value eventMisc)
{
    if (nullptr != mBilling)
    {
        return mBilling->logEventSync(eventId, attemptId, eventMisc);
    }

    return false;
}

bool ORMSQLite::escrowRedeemScan(string scanType, string token, string scanID)
{
    bool bResult = false;

    JCallBack *pCb = ORMSQLite::getInstance()->objectFind("escrowRedeemScan");
    if (nullptr != pCb)
    {
        AutoJniEnv env;
        AutoJniFrame(env, 1);
        jstring jScanTypeStr = env->NewStringUTF(scanType.c_str());
        jstring jTokenStr = env->NewStringUTF(token.c_str());
        jstring jScanIDStr = env->NewStringUTF(scanID.c_str());
        bResult = env->CallBooleanMethod(pCb->mObject, pCb->getMethod("escrowRedeemScan"), jScanTypeStr, jTokenStr, jScanIDStr);
        env->DeleteLocalRef(jScanTypeStr);
        env->DeleteLocalRef(jTokenStr);
        env->DeleteLocalRef(jScanIDStr);
    }

    return bResult;
}
