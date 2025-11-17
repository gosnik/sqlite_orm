//
// Created by aphoenix on 07-Apr-16.
//
#pragma once

#include "Types.hpp"
#include "Mutex.hpp"
#include "Sqlite.hpp"
#include "Thread.hpp"
#include "Condition.hpp"
#include "Utils.hpp"
#include "Base64.hpp"

// Flag for login required -
#define FLAG_LOGIN_PINCOUNT         1

typedef MAPTYPE<string, JCallBack*> pjcallback_map;
typedef MAPTYPE<string, Schema*> pschema_map;
typedef MAPTYPE<string, std::shared_ptr<Sqlite>> db_map;
typedef MAPTYPE<char, vector<string>> db_pending_map;
typedef MAPTYPE<string, string> string_map;

class DLL_EXPORTED ORMSQLite : public Thread
{
public:
    static ORMSQLite*                       pThis;

    string                                  mBaseUrl;
    string                                  mBaseFolder;
    string                                  mDeviceId;
    string                                  mUsername;
    string                                  mGuestUsername;
    string                                  mCommandLine;

    string                                  mToken_aid;
    string                                  mToken_cid;
    string                                  mToken_created;
    string                                  mToken_env;
    string                                  mToken_vid;
    string                                  mSecret;
    char                                    mState;
    string_map                              mTempStore;

    Mutex 									mMutex;
    Condition                               mCondition;
    pjcallback_map	                        mObjMap;
    pschema_map	                            mSchemas;
    string_map                              mFlags;
    std::vector<NotifyQueueItem>            mQueue;
    std::vector<string>                     mCertificates;
    Api                                     mApi;

	db_map                                  mDbMap;
	db_pending_map                          mDbPendingMap;

    bool                                    mInit;
    bool                                    mInitComplete;
    bool                                    mRun;
    bool                                    mRunApi;
    bool                                    mPendingStarted;

    static ORMSQLite*   getInstance();
    static JavaVM* 		getVm();

    void init(const char*, const char*, AAssetManager*);
    bool init() { return mInit; };

    void postInit();

    //TODO:
    void signIn();

    std::string getBaseFolder() {return mBaseFolder;}
    AndroidAssetManager* getAssetManager() {return mAndroidAssetMgr;};

    void initBilling(const char* url, const char* key);
    Billing* getBilling() {return mBilling;};
    bool billing_sync(bool bAsync = true);
    bool billing_eventPending(std::string field, std::string value);
    bool billing_logEvent(BillingEventId eventId, std::string attemptId = "", Json::Value eventMisc = Json::Value());
    bool billing_logEventSync(BillingEventId eventId, std::string attemptId = "", Json::Value eventMisc = Json::Value());

    void dbOpened(const std::string&, sqlite3*);
    void dbClosed(sqlite3* db);
    void dbClose(std::string path);

    std::shared_ptr<Sqlite> getDb(char type);
    std::shared_ptr<Sqlite> getDb(Schema*);
    std::shared_ptr<Sqlite> getDb(Schema*, std::string);
    std::shared_ptr<Sqlite> getDb(std::string, bool);
    void checkpoint(char type);
    bool checkTries();

    void setPassword(const char *, const char *);
    void changePassword(const char*);
    bool checkPassword();

    Api& getApi();

    void addSchema(string tableStr, string colsStr, string typesStr, string anosStr, string type);
    Schema* getSchema(string name);

    void objectRegister(JNIEnv* env, const char*, const char*, const char*, jobject);
    JCallBack* objectFind(const char* name);
    void objectClear();

    // inlined for security.
    inline std::string getApiKey(int apiIx)
    {
        switch (apiIx)
        {
            case APIIX_DEFAULT:
            {
                return "";//return Utils::decodeStr(STR_apiKeyD);
            }

            case APIIX_OTHER:
            {
                return "";//return Utils::decodeStr(STR_apiKeyTradie);
            }

            default:
            {
                return "";
            }
        }
    };

    // inlined for security.
    inline std::string getApiSecret(int apiIx)
    {
        switch (apiIx)
        {
            case APIIX_DEFAULT:
            {
                return "";
            }

            case APIIX_OTHER:
            {
                return "";//return Utils::decodeStr(STR_apiSecretTradie);
            }

            default:
            {
                return "";
            }
        }
    }

    // inlined for security.
    inline std::string getApiUsername(int apiIx)
    {
        switch (apiIx)
        {
            case APIIX_DEFAULT:
            {
                return "";
            }

            case APIIX_OTHER:
            {
                return "";//return Utils::decodeStr(STR_apiUsernameTradie);
            }

            default:
            {
                return "";
            }
        }
    }

    // inlined for security.
    inline std::string getApiPassword(int apiIx)
    {
        switch (apiIx)
        {
            case APIIX_DEFAULT:
            {
                return "";
            }

            case APIIX_OTHER:
            {
                return "";//return Utils::decodeStr(STR_apiPasswordTradie);
            }

            default:
            {
                return "";
            }
        }
    }

    inline std::string getApiId(int apiIx, bool bWithIdCol = true)
    {
        std::ostringstream idStm;

        if (bWithIdCol)
            idStm << "id=";

        idStm << apiIx;

        return idStm.str();
    }

    inline std::string getApiSettingName(int apiIx, const char* prefix)
    {
        std::ostringstream idStm;

        idStm << prefix << apiIx;
        return idStm.str();
    }

    inline std::string getApiAuth(int apiIx)
    {
        std::ostringstream authStm;
        std::string authStr;

        authStm << getApiKey(apiIx);
        authStm << ":";
        authStm << getApiSecret(apiIx);
        authStr = authStm.str();

        return Base64::getInstance()->base64_encode(authStr.c_str(), authStr.length(), false, true);
    }

    // inlined for security.
    inline std::string getSnswSecret()
    {
#if (BUILDTYPE > 1)
        return Utils::decodeStr(STR_macSecretD);
#else
    #if (BUILDTYPE > 0)
        return "";//return Utils::decodeStr(STR_macSecretP);
    #else
        return "";//return Utils::decodeStr(STR_macSecretR);
    #endif
#endif
    };

    string getUrl(const char*, int);
    string getLastResponse(const char*);
    void setLastResponse(const char*, const char*, std::string);

    bool isApiEnabled()
    {
        return mRunApi;
    }

    void setApiEnabled(bool bEnabled)
    {
        mRunApi = bEnabled;
    }

    void setAccessToken(int, field_map&);
    void callTokenRefreshed();
    void callLoginRequired(int);
    string getUser();

    std::string getToken(int);
    std::string getEnvironment(int);
    std::string getAppId(int);
    std::string getVendorId(int);
    std::string getSmallbatch(int);
    std::string getBinaryKeys();
    std::string getResourceUrl(int);
    std::string getInterfaceUrl(int);

    void pushExecPending(char dbNo, const string& data);
    void execPending();
    void doExecPending();

    bool getSetting(const char* name, string& textVal);
    void putSettingStr(const char* name, string textVal);

    void initNdkDatabase();

    void initKeyStore();
    bool getKey(std::string name, string& val);
    void setKey(std::string name, string val);

    void initEtagStore();
    bool getEtag(std::string name, string& val);
    void setEtag(std::string name, string val);

    void getSetting(const char* name, string& textVal, uint64_t& longVal, uint64_t& longEnabled, uint64_t& timeStamp);
    void putSetting(const char* name, string textVal, uint64_t longVal, uint64_t longEnabled, uint64_t timeStamp);
    void putSettingPending(const char* name, string textVal, uint64_t longVal, uint64_t longEnabled, uint64_t timeStamp,
                           bool bPending = true);

    bool isNetworkAvailable();

    bool hasCertKeys(string&, string&);
    int createCertificate();
    bool hasCertificate(const char*, const char*);
    void handleCertResponse(int responseCode, int&, string&, const char*, const char*);
    string readCertificate(const char* certKey);
    string stripGuard(string& src);
    string getDeviceId();

    void notifyTableChanged(const char* table, const char* id);
    void run(void*);

    void readCertificates(const char*, size_t);
    void readCertificateFile(const char*);
    std::vector<string>& getCertificates();

    void setIntegerValue(jobject obj, int value);

    void setFlag(const char*, const char*);
    bool hasFlag(const char*);
    std::string getFlag(const char*);
    bool getFlagBool(const char*, bool bDefault = false);

    int getPoseFrameCnt();
	
	void setGuest(const char *);
    std::string getGuest();

    void setEnv(std::string, std::string, std::string, std::string, std::string);
    void setEnv(vector<std::string>&);

    void setSecret(std::string);
    std::string getSecret();

    void setState(char);
    char getState();
    bool stateCapture();

    bool hasTokens();
    std::string getToken_aid();
    std::string getToken_cid();
    std::string getToken_created();
    std::string getToken_env();
    std::string getToken_vid();
    std::string getToken_uid();

    void apiProgressCallback(std::string,
                         curl_off_t dltotal,
                         curl_off_t dlnow,
                         curl_off_t ultotal,
                         curl_off_t ulnow);

    std::string getAsset(std::string name);
    std::string getAssetIfReady(std::string name);
    std::string getUuid();
    std::string getUserId();
    std::string getUserIdHash();

    void uploadPayload(std::string name);
    bool escrowRedeemScan(string scanType, string token, string scanID);

private:
    ORMSQLite();
    ~ORMSQLite();

    void doNotifyTableChanged(const string& table);
    void queueNotifyTableChanged(const char* table, const char* id);
};
