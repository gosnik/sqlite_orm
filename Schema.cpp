//
// Created by Adam.Phoenix on 11/11/2015.
//

#include "Schema.hpp"
#include "Utils.hpp"
#include "Base64.hpp"
#include "ORMSQLite.hpp"
#include "AutoSqlite.hpp"

#define LOG_TAG "ORM"

string Schema::mModelPrefix = "model.";
string Schema::mModelListPrefix = "list.";

Schema::Schema(string tableStr, string colsStr, string typesStr, string anosStr, string type)
        : mParents(), mTable(tableStr), mCols(), mTypes(), mAnos(), mType(type), mHasCustomId(false)
{
    vector<string> anoList;
    std::vector<string>::iterator it;

    Utils::split(colsStr, ',', mCols);
    Utils::split(typesStr, ',', mTypes);
    Utils::split(anosStr, ',', anoList);

    //LOG_INFO(LOG_SCHEMA, "[Schema::Schema] %s anos:%s", mTable.c_str(), anosStr.c_str());

    for (it = anoList.begin(); it != anoList.end(); it++)
    {
        vector<string> anoFields;
        Utils::split(*it, ':', anoFields);
        mAnos.push_back(anoFields);
    }

    size_t count = mAnos.size();
    for (size_t i = 0; i < count; i++)
    {
        if (getAnnotationBool("idMap=", mAnos[i]))
        {
            mHasCustomId = true;
            break;
        }
    }
}

Schema::~Schema()
{
}

string Schema::getType()
{
    return mType;
}

char Schema::getDbType()
{
    // get the last character
    return mType[mType.length() - 1];
}

const char *Schema::getTable()
{
    return mTable.c_str();
}

void Schema::getValues(field_map &data, const Value &values, string &parseTime)
{
    //LOG_INFO(LOG_SCHEMA, "[Schema::getValues] %s", mTable.c_str());
    vector<string> parent_ano;

    size_t count = mCols.size();

    //LOG_INFO(LOG_SCHEMA, "[Schema::getValues] col count: %d", count);

    for (size_t i = 0; i < count; i++)
    {
        //LOG_INFO(LOG_SCHEMA, "getValues:%s", mCols[i].c_str());
        getValue(data, mCols[i], mTypes[i], mAnos[i], values, values, parseTime, parent_ano);
    }
}

void Schema::getValues(field_map &data,
                       const Value &rootValue,
                       const Value &values,
                       string &parseTime,
                       vector<string> &parent_ano, const char *parentId, const char *childIdMap)
{
    size_t count = mCols.size();
    for (size_t i = 0; i < count; i++)
    {
        getValue(data, mCols[i], mTypes[i], mAnos[i], rootValue, values, parseTime, parent_ano, parentId, childIdMap);
    }
}

void Schema::putValue(field_map &data, const char *name, std::string value, bool bModified)
{
    putValue(data, name, value.c_str(), bModified);
}

void Schema::putValue(field_map &data, const char *name, const char *value, bool bModified)
{
    bool bFound = false;
    size_t count = mCols.size();
    for (size_t i = 0; i < count; i++)
    {
        if (mCols[i] == name)
        {
            string s(value);
            string field(name);

            bFound = true;

            if (mTypes[i] == "string")
            {
                //LOG_INFO(LOG_SCHEMA, "[Schema::putValue] found %s %s", name, value);
                data[field] = Field(s, mTypes[i], true, bModified);
            }
            else if (mTypes[i] == "b91json")
            {
                //LOG_INFO(LOG_SCHEMA, "[Schema::putValue] found %s %s", name, value);
                std::string decoded;
                Utils::decodeBase91(decoded, s, Format::B91STRING);
                data[field] = Field(decoded, mTypes[i], true, bModified);
            }
            else if (mTypes[i] == "bool")
            {
                //LOG_INFO(LOG_SCHEMA, "[Schema::putValue] found bool %s %s", name, value);
                if (string::npos != s.find("1"))
                {
                    data[field] = Field("1", mTypes[i], true, bModified);
                }
                else if (string::npos != s.find("true"))
                {
                    data[field] = Field("1", mTypes[i], true, bModified);
                }
                else
                {
                    data[field] = Field("0", mTypes[i], true, bModified);
                }
            }
            else if (mTypes[i] == "long")
            {
                //LOG_INFO(LOG_SCHEMA, "[Schema::putValue] found long %s %s", name, value);
                if (s.length() < 1)
                {
                    s = "0";
                }
                data[field] = Field(s, mTypes[i], true, bModified);
            }
            else if (mTypes[i] == "int")
            {
                if (s.length() < 1)
                {
                    s = "0";
                }
                data[field] = Field(s, mTypes[i], true, bModified);
            }
            else if (mTypes[i] == "float")
            {
                if (s.length() < 1)
                {
                    s = "0";
                }
                data[field] = Field(s, mTypes[i], true, bModified);
            }
            else if (mTypes[i] == "double")
            {
                if (s.length() < 1)
                {
                    s = "0";
                }
                data[field] = Field(s, mTypes[i], true, bModified);
            }
            else if (mTypes[i] == "enum")
            {
                if (s.length() < 1)
                {
                    s = "0";
                }
                data[field] = Field(s, mTypes[i], true, bModified);
            }
            else if (0 == mTypes[i].compare(0, mModelPrefix.length(), mModelPrefix))
            {
                escapeStr(s);
                data[field] = Field(s, mTypes[i], true, bModified);
            }
            else if (0 == mTypes[i].compare(0, mModelListPrefix.length(), mModelListPrefix))
            {
                escapeStr(s);
                data[field] = Field(s, mTypes[i], true, bModified);
            }

            if (getAnnotationBool("idMap=", mAnos[i]))
            {
                //LOG_INFO(LOG_SCHEMA, "[Schema::putValue] idmap %s %s", name, value);
                data["id"] = data[field];
            }

            break;
        }
    }

    if (!bFound)
    {
        LOG_INFO(LOG_SCHEMA, "[Schema::putValue] FAILED: %s %s", name, value);
    }
}

void Schema::putValue(field_map &data, const char *name, int64_t value, bool bModified)
{
    //LOG_INFO(LOG_SCHEMA, "[Schema::putValue] %s %ld", name, value);

    size_t count = mCols.size();
    for (size_t i = 0; i < count; i++)
    {
        if (mCols[i] == name)
        {
            // get value as a string.
            ostringstream tempStrm;
            tempStrm << value;

            string s(tempStrm.str());
            string field(name);

            if (mTypes[i] == "bool")
            {
                //LOG_INFO(LOG_SCHEMA, "[Schema::putValue] found bool %s %ld", name, value);
                data[field] = Field(
                        (string::npos != s.find("true")
                         ? "1"
                         : "0"),
                        mTypes[i], true, bModified);
            }
            else if (mTypes[i] == "long")
            {
                //LOG_INFO(LOG_SCHEMA, "[Schema::putValue] found long %s %ld", name, value);
                if (s.length() < 1)
                {
                    s = "0";
                }
                data[field] = Field(s, mTypes[i], true, bModified);
            }

            if (getAnnotationBool("idMap=", mAnos[i]))
            {
                //LOG_INFO(LOG_SCHEMA, "[Schema::putValue] idmap %s %ld", name, value);
                data["id"] = data[field];
            }
            break;
        }
    }
}

void Schema::putValueFloat(field_map &data, const char *name, float value, bool bModified)
{
    size_t count = mCols.size();
    for (size_t i = 0; i < count; i++)
    {
        if (mCols[i] == name)
        {
            // get value as a string.
            ostringstream tempStrm;
            tempStrm << value;

            string s(tempStrm.str());
            string field(name);

            if (mTypes[i] == "float")
            {
                if (s.length() < 1)
                {
                    s = "0";
                }
                data[field] = Field(s, mTypes[i], true, bModified);
            }
            else if (mTypes[i] == "double")
            {
                if (s.length() < 1)
                {
                    s = "0";
                }
                data[field] = Field(s, mTypes[i], true, bModified);
            }

            if (getAnnotationBool("idMap=", mAnos[i]))
            {
                data["id"] = data[field];
            }
            break;
        }
    }
}

void Schema::printValues(const char *header, field_map &data)
{
    LOG_INFO(LOG_SCHEMA, "------ start printValues %s ------", header);
    for (field_map::iterator iter = data.begin(); iter != data.end(); ++iter)
    {
        LOG_INFO(LOG_SCHEMA, "printValues %s -> %s", iter->first.c_str(), iter->second.getPrint().c_str());
    }
    LOG_INFO(LOG_SCHEMA, "------ end printValues %s ------", header);
}

bool Schema::isUpdateNeeded(field_map &data)
{
    for (field_map::iterator iter = data.begin(); iter != data.end(); ++iter)
    {
        if (iter->second.isModified())
        {
            return true;
        }
    }

    return false;
}

void Schema::setUpdateNeeded(field_map &data, bool bNeeded)
{
    for (field_map::iterator iter = data.begin(); iter != data.end(); ++iter)
    {
        iter->second.setModified(bNeeded);
    }
}

void Schema::getUpdate(ostringstream &update, field_map &data)
{
    string where = "id=" + data["id"].get();
    getUpdate(update, where.c_str(), data);
}

void Schema::getUpdate(ostringstream &update, const char *where, field_map &data)
{
    update << "UPDATE " << mTable << " set ";

    size_t size = data.size();
    bool bFirst = true;
    for (field_map::iterator iter = data.begin(); iter != data.end(); ++iter)
    {
        if (iter->second.isModified())
        {
            if (!bFirst)
            {
                update << ",";
            }
            update << iter->first << "=" << iter->second.getStore();
            bFirst = false;
        }
    }

    update << " WHERE " << where << ";";
}

void Schema::getInsert(ostringstream &update, field_map &data, bool bOrIgnore)
{
    if (!bOrIgnore)
        update << "INSERT INTO " << mTable << " (";
    else
        update << "INSERT OR IGNORE INTO " << mTable << " (";

    size_t size = data.size();
    bool bFirst = true;

    for (field_map::iterator iter = data.begin(); iter != data.end(); ++iter)
    {
        if (iter->second.isInSchema())
        {
            if (!bFirst)
            {
                update << ",";
            }

            update << iter->first;
            bFirst = false;
        }
    }

    update << ") VALUES (";
    bFirst = true;
    for (field_map::iterator iter = data.begin(); iter != data.end(); ++iter)
    {
        if (iter->second.isInSchema())
        {
            if (!bFirst)
            {
                update << ",";
            }

            update << iter->second.getStore();
            bFirst = false;
        }
    }
    update << ");";
}

void Schema::getUpsert(ostringstream &update, field_map &data)
{
    string where = "id=" + data["id"].get();
    getUpsert(update, where.c_str(), data);
}

void Schema::getUpsert(ostringstream &update, const char *where, field_map &data)
{
    getUpdate(update, where, data);
    getInsert(update, data, true);
    update << ";";
}

bool Schema::getValue(
        field_map &data, string &field, string &type, vector<string> &ano,
        const Value &rootValue,
        const Value &value,
        string &parseTime,
        vector<string> &parent_ano,
        const char *parentId, const char *childIdMap)
{
    //LOG_INFO(LOG_SCHEMA, "getValue: %s %s", field.c_str(), type.c_str());

    Value myValue(value);
    string myField = field;
    string childIdMapValue;
    string parentIdValue;
    bool bFound = false;
    bool bIdMap = false;
    bool bIsJwt = false;
    bool bIsEscaped = false;
    bool bExplicit = false;
    bool asReference = false;
    bool bPk = false;
    bool bMask = false;
    bool bFromParentValue = false;
    bool bFromParent = false;
    bool bDbPath = false;

    // ignore id if custom id defined.
    if ((field != "id") || (!mHasCustomId))
    {
        bPk = getAnnotationBool("pk=", ano);

        // handle pk - ignore...
        if (!bPk)
        {
            bIdMap = getAnnotationBool("idMap=", ano);
            bExplicit = getAnnotationBool("explicit=", ano);
            bIsJwt = getAnnotationBool("isJwt=", ano);
            bIsEscaped = getAnnotationBool("escaped=", ano);
            asReference = getAnnotationBool("asReference=", ano);
            bMask = getAnnotationBool("mask=", ano);
            bFromParent = getAnnotationBool("fromParent=", ano);
            bFromParentValue = getAnnotationBool("fromParentValue=", ano);
            bDbPath = getAnnotationBool("dbPath=", ano);

            // handle jsonMap...
            string jsonMap = getAnnotation("jsonMap=", ano);

            // handle from root...
            if (bFromParentValue)
            {
                myValue = rootValue;
            }

            // if passed childIdMap is not null and not empty...
            if ((nullptr != childIdMap) && (childIdMap[0] != '\0'))
                childIdMapValue = childIdMap;
            else
                childIdMapValue = getAnnotation("childIdMap=", ano);

            if (nullptr != parentId)
                parentIdValue = parentId;

            //LOG_INFO(LOG_SCHEMA, "getValue [%s] -> %s isImage=%d idMap=%d explicit=%d jsonMap=%s",
            //         mTable.c_str(), field.c_str(), (bImage?1:0), (bIdMap?1:0), (bExplicit?1:0), jsonMap.c_str());

            if (jsonMap.length() > 0)
            {
                //LOG_INFO(LOG_SCHEMA, "getValue map %s -> jsonMap: %s", field.c_str(), jsonMap.c_str());

                vector<std::string> v;
                Utils::split(jsonMap, '!', v);
                size_t nElem = v.size();
                for (size_t i = 0; i < nElem; i++)
                {
                    if (i < nElem - 1)
                    {
                        // get sub object from value.
                        // only drill down if the parent item exists...
                        // this allows the match to be found even if the parent doesn't exist.
                        if (myValue.isMember(v[i]))
                        {
                            // This 'hack' allows us to drill down in to a sub-array item and get
                            // data from the first item as part of the parent item.
                            if (myValue[v[i]].isArray())
                            {
                                //LOG_INFO(LOG_SCHEMA, "getValue array [%s] sub item: %s -> jsonMap: %s", mTable.c_str(), field.c_str(), v[i].c_str());
                                myValue.swap(myValue[v[i]][0u]);
                            }
                            else
                            {
                                //LOG_INFO(LOG_SCHEMA, "getValue [%s] sub item: %s -> jsonMap: %s", mTable.c_str(), field.c_str(), v[i].c_str());
                                myValue.swap(myValue[v[i]]);
                            }
                        }
                        else if (bExplicit)
                        {
                            //LOG_INFO(LOG_SCHEMA, "getValue [%s] sub item error - skipping: %s -> jsonMap: %s", mTable.c_str(), field.c_str(), v[i].c_str());
                            return false;
                        }
                    }
                    else
                    {
                        //LOG_INFO(LOG_SCHEMA, "getValue [%s] item: %s -> jsonMap: %s (%s)", mTable.c_str(), field.c_str(), jsonMap.c_str(), v[i].c_str());
                        // get value as string.
                        myField = v[i];
                    }
                }

                // This 'hack' allows us to drill down in to a sub-array item and get
                // data from the first item as part of the parent item.
                if ((myValue.isArray()) && (myValue.size() > 0))
                {
                    //LOG_INFO(LOG_SCHEMA, "getValue array [%s] sub item: %s", mTable.c_str(), myField.c_str());
                    myValue = myValue[0u];
                }
            }

            if (type == "timestamp")
            {
                data[field] = Field(Utils::timeInMillis(), "long");
                bFound = true;
            }
            else if (type == "synctime")
            {
                data[field] = Field(parseTime, "long");
                bFound = true;
            }
            else if (type == "updatedtime")
            {
                data[field] = Field(parseTime, "long");
                bFound = true;
            }
            else if (bFromParent)
            {
                data[field] = Field(parentIdValue, type);

                //LOG_INFO(LOG_SCHEMA, "[Schema::getValue] handle model from parent: %s (%s)",
                //         type.c_str(), parentIdValue.c_str());
                bFound = true;
            }

            if (!myValue.isNull())
            {
                bool bFoundJson = false;

                // Try to find case sensitive...
                if (!myValue.isMember(myField))
                {
                    // convert to lowercase...
                    std::transform(myField.begin(), myField.end(), myField.begin(),
                                   [](unsigned char c) { return std::tolower(c); });

                    // try to find again...
                    bFoundJson = myValue.isMember(myField);

                    if (!myValue.isMember(myField))
                    {
                        // convert underscore to hyphen
                        std::transform(myField.begin(), myField.end(), myField.begin(),
                                       [](unsigned char c) { return c == '_' ? '-' : c; });

                        // try to find again...
                        bFoundJson = myValue.isMember(myField);
                    }
                    else
                    {
                        bFoundJson = true;
                    }
                }
                else
                {
                    bFoundJson = true;
                }

                if (bFoundJson)
                {
                    //LOG_INFO(LOG_SCHEMA, "getValue got [%s] -> %s", mTable.c_str(), field.c_str());

                    if ((type == "string") || (type == "enum"))
                    {
                        string s(myValue.get(myField, "").asString());
                        escapeStr(s);
                        data[field] = Field(s, type);
                        bFound = true;
                    }
                    else if (type == "b91json")
                    {
                        // Get value (and any child values) as a string and store
                        Json::FastWriter writer;
                        std::string result = writer.write(myValue.get(myField, ""));
                        data[field] = Field(result, type);
                        bFound = true;
                    }
                    else if (type == "bool")
                    {
                        string v = myValue.get(myField, "false").asString();
                        std::transform(v.begin(), v.end(), v.begin(), ::tolower);
                        data[field] = Field(
                                (string::npos != v.find("true")
                                 ? "1"
                                 : "0"),
                                type);
                        bFound = true;
                    }
                    else if (type == "long")
                    {
                        string v = myValue.get(myField, "0").asString();
                        if (v.length() < 1)
                        {
                            v = "0";
                        }
                        data[field] = Field(v, type);
                        bFound = true;
                    }
                    else if (type == "int")
                    {
                        string v = myValue.get(myField, "0").asString();
                        if (v.length() < 1)
                        {
                            v = "0";
                        }
                        data[field] = Field(v, type);
                        bFound = true;
                    }
                    else if (type == "double")
                    {
                        string v = myValue.get(myField, "0").asString();
                        if (v.length() < 1)
                        {
                            v = "0";
                        }
                        data[field] = Field(v, type);
                        bFound = true;
                    }
                    else if (type == "float")
                    {
                        string v = myValue.get(myField, "0").asString();
                        if (v.length() < 1)
                        {
                            v = "0";
                        }
                        data[field] = Field(v, type);
                        bFound = true;
                    }
                    // type starts with model.
                    else if (0 == type.compare(0, mModelPrefix.length(), mModelPrefix))
                    {
                        if (asReference)
                        {
                            string s(myValue.get(myField, "").asString());
                            escapeStr(s);
                            data[field] = Field(s, type);

                            //LOG_INFO(LOG_SCHEMA, "[Schema::getValue] handle model as reference: %s (%s)",
                            //         type.c_str(), s.c_str());
                            bFound = true;
                        }
                        else
                        {
                            string id;
                            string modelType = type.substr(mModelPrefix.length());
                            Schema *pSchema = ORMSQLite::getInstance()->getSchema(modelType);
                            //LOG_INFO(LOG_SCHEMA, "[Schema::getValue] handle model: %s",
                            //         modelType.c_str());
                            if (nullptr != pSchema)
                            {
                                if (data.end() != data.find("id"))
                                {
                                    id = data["id"].getRaw();
                                }
                                if (bIsJwt)
                                {
                                    std::vector<std::string> results = Utils::split(
                                            myValue[myField].asString(), '.');
                                    if (results.size() > 2)
                                    {
                                        string jwt_body = results[1];
                                        string parse_data = Base64::getInstance()->base64_decode_toStr(
                                                jwt_body);
                                        //Utils::trace("[Schema::getValue] got JWT body:",
                                        //             parse_data.c_str());
                                        string v = pSchema->parseAndStore(
                                                nullptr,
                                                parse_data,
                                                ano,
                                                parseTime,
                                                id.c_str(), childIdMapValue.c_str());

                                        if (v.length() < 1)
                                        {
                                            v = "0";
                                        }
                                        data[field] = Field(v, type);
                                        bFound = true;
                                    }
                                }
                                else if (bIsEscaped)
                                {
                                    Value root;
                                    Reader reader;
                                    string escapedValue = myValue[myField].asString();
                                    Utils::str_replace(escapedValue, "\\", "");

                                    if (reader.parse(escapedValue, root, false))
                                    {
                                        string v = pSchema->parseAndStore(
                                                nullptr,
                                                value,
                                                root,
                                                parseTime,
                                                ano,
                                                id.c_str(),
                                                childIdMapValue.c_str());

                                        if (v.length() < 1)
                                        {
                                            v = "0";
                                        }
                                        data[field] = Field(v, type);
                                        bFound = true;
                                    }
                                    else
                                    {
                                        LOG_ERROR(LOG_GLOBAL, "[Schema::getValue] Error parsing %s: %s",
                                                  mTable.c_str(), escapedValue.c_str());
                                    }
                                }
                                else
                                {
                                    string v = pSchema->parseAndStore(
                                            nullptr,
                                            value,
                                            myValue[myField],
                                            parseTime,
                                            ano,
                                            id.c_str(), childIdMapValue.c_str());

                                    if (v.length() < 1)
                                    {
                                        v = "0";
                                    }
                                    data[field] = Field(v, type);
                                    bFound = true;
                                }
                            }
                        }
                    }
                        // type starts with list.
                    else if (0 == type.compare(0, mModelListPrefix.length(), mModelListPrefix))
                    {
                        if (asReference)
                        {
                            Value subItems;
                            std::vector<std::string> childIds;
                            subItems.swap(myValue[myField]);
                            bool bFirst = true;
                            ostringstream items;

                            for (int index = 0; index < subItems.size(); index++)
                            {
                                if (subItems[index].isMember("id"))
                                {
                                    string childId(subItems[index].get("id", "").asString());
                                    escapeStr(childId);

                                    if (std::find(childIds.begin(), childIds.end(), childId) ==
                                        childIds.end())
                                    {
                                        childIds.push_back(childId);
                                    }

                                    //LOG_INFO(LOG_SCHEMA,
                                    //         "[Schema::getValue] handle model array item as reference: %s (%s)",
                                    //         type.c_str(), childId.c_str());
                                }
                            }

                            for (vector<string>::iterator it = childIds.begin();
                                 it != childIds.end(); it++)
                            {
                                if (!bFirst)
                                    items << ((char) 1);
                                bFirst = false;
                                items << (*it);
                            }

                            data[field] = Field(items.str());
                            bFound = true;
                        }
                        else
                        {
                            string modelType = type.substr(mModelListPrefix.length());
                            Schema *pSchema = ORMSQLite::getInstance()->getSchema(modelType);
                            bool bDelNotInSet = getAnnotationBool("delNotInSet=", ano);

                            string id;

                            //LOG_INFO(LOG_SCHEMA, "[Schema::getValue] handle model list: (bDel:%d) %s",
                            //         (bDelNotInSet ? 1 : 0), modelType.c_str());

                            if (nullptr != pSchema)
                            {
                                Value subItems;
                                std::vector<std::string> childIds;
                                subItems.swap(myValue[myField]);

                                bool bFirst = true;
                                ostringstream items;

                                AutoSqlite sqlite(ORMSQLite::getInstance()->getDb(pSchema));
                                if (sqlite.isOpen())
                                {
                                    if (data.end() != data.find("id"))
                                    {
                                        id = data["id"].getRaw();
                                    }

                                    if (!bDelNotInSet)
                                    {
                                        AutoSqlite mysqlite(ORMSQLite::getInstance()->getDb(this));
                                        if (mysqlite.isOpen())
                                        {
                                            std::ostringstream dest;
                                            std::ostringstream whereStm;

                                            whereStm << "id='" << id << "'";
                                            string where = whereStm.str();

                                            string value = mysqlite.getValue(getTable(), field.c_str(),
                                                                             where.c_str());
                                            Utils::split(value, ((char) 1), childIds);

                                            //for (vector<string>::iterator it = childIds.begin(); it != childIds.end(); it++)
                                            //{
                                            //    LOG_INFO(LOG_GLOBAL, "HAS CHILD %s", (*it).c_str());
                                            //}
                                        }
                                    }

                                    for (int index = 0; index < subItems.size(); index++)
                                    {
                                        if (bIsJwt)
                                        {
                                            std::vector<std::string> results;
                                            Utils::split(subItems[index].asString(), '.', results);

                                            if (results.size() > 2)
                                            {
                                                const string &jwt_body = results[1];

                                                string parse_data = Base64::getInstance()->base64_decode_toStr(
                                                        jwt_body);
                                                //Utils::trace("[Schema::getValue] got JWT body:",
                                                //             parse_data.c_str());

                                                string childId = pSchema->parseAndStore(
                                                        &sqlite,
                                                        parse_data,
                                                        ano,
                                                        parseTime,
                                                        id.c_str(), childIdMapValue.c_str());

                                                if (std::find(childIds.begin(), childIds.end(),
                                                              childId) ==
                                                    childIds.end())
                                                {
                                                    childIds.push_back(childId);
                                                }
                                            }
                                        }
                                        else if (bIsEscaped)
                                        {
                                            Value root;
                                            Reader reader;
                                            string escapedValue = subItems[index].asString();
                                            Utils::str_replace(escapedValue, "\\", "");

                                            if (reader.parse(escapedValue, root, false))
                                            {
                                                string childId = pSchema->parseAndStore(
                                                        &sqlite,
                                                        value,
                                                        root,
                                                        parseTime,
                                                        ano,
                                                        id.c_str(),
                                                        childIdMapValue.c_str());

                                                if (std::find(childIds.begin(), childIds.end(),
                                                              childId) ==
                                                    childIds.end())
                                                {
                                                    childIds.push_back(childId);
                                                }
                                            }
                                            else
                                            {
                                                LOG_ERROR(LOG_GLOBAL,
                                                          "[Schema::getValue] Error parsing %s: %s",
                                                          mTable.c_str(), escapedValue.c_str());
                                            }
                                        }
                                        else
                                        {
                                            string childId = pSchema->parseAndStore(
                                                    &sqlite,
                                                    value,
                                                    subItems[index],
                                                    parseTime,
                                                    ano,
                                                    id.c_str(), childIdMapValue.c_str());

                                            if (std::find(childIds.begin(), childIds.end(), childId) ==
                                                childIds.end())
                                            {
                                                childIds.push_back(childId);
                                            }
                                        }
                                    }

                                    for (vector<string>::iterator it = childIds.begin();
                                         it != childIds.end(); it++)
                                    {
                                        if (!bFirst)
                                            items << ((char) 1);
                                        bFirst = false;
                                        items << (*it);

                                        //LOG_INFO(LOG_GLOBAL, "HAS NEW CHILD %s", (*it).c_str());
                                    }

                                    data[field] = Field(items.str());
                                    bFound = true;

                                    if (bDelNotInSet)
                                    {
                                        int deleted = sqlite.del(pSchema->getTable(), childIds);

                                        if (deleted > 0)
                                        {
                                            LOG_INFO(LOG_SCHEMA, "Deleted %d items from %s", deleted,
                                                     pSchema->getTable());
                                            pSchema->notifyTableChanged(id.c_str());
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if (bFound)
                    {
                        // handle idMap
                        if (bIdMap)
                        {
                            string id;
                            if (data.end() != data.find("id"))
                            {
                                id = data["id"].getRaw();
                            }
                            id.append(data[field].getRaw());
                            data["id"] = Field(id);
                            data["id"].setInSchema(true);
                            //LOG_INFO(LOG_SCHEMA, "getValue idMap: %s -> %s", field.c_str(),
                            //         data[field].get().c_str());
                        }

                        // handle childIdMap
                        if ((childIdMapValue.length() > 0) && (childIdMapValue == field))
                        {
                            string id;
                            id.append(parentIdValue);
                            id.append(data[field].getRaw());
                            data["id"] = Field(id);
                            data["id"].setInSchema(true);
                            //LOG_INFO(LOG_SCHEMA, "getValue childIdMap: %s -> %s", field.c_str(),
                            //         id.c_str());
                        }

                        if (bMask)
                        {
                            string maskChar = getAnnotation("maskChar=", ano);
                            data[field].maskValue(maskChar);
                        }

                        if (bDbPath)
                        {
                            data[field].setIsDbPath(bDbPath);
                        }
                    }
                }
            }

            // do column check.
            for (field_map::iterator iter = data.begin(); iter != data.end(); ++iter)
            {
                if (iter->first == field)
                {
                    //LOG_INFO(LOG_SCHEMA, "getValue found field in data: %s", field.c_str());
                    iter->second.setInSchema(true);
                }
            }
        }
    }
    return bFound;
}

bool Schema::getAnnotationBool(const char *annotation, vector<string> &ano_fields)
{
    string ano = getAnnotation(annotation, ano_fields);
    if (ano.length() > 0)
    {
        return (ano.c_str()[0] == '1');
    }
    return false;
}

string Schema::getAnnotation(const char *annotation, vector<string> &ano_fields)
{
    std::vector<string>::iterator it;

    //LOG_INFO(LOG_SCHEMA, "getAnnotation %s", annotation);

    for (it = ano_fields.begin(); it != ano_fields.end(); it++)
    {
        //LOG_INFO(LOG_SCHEMA, "getAnnotation field: %s", (*it).c_str());
        if (string::npos != (*it).find(annotation))
        {
            string val = *it;
            Utils::str_replace(val, annotation, "");
            //LOG_INFO(LOG_SCHEMA, "getAnnotation got: %s -> %s", annotation, val.c_str());
            return val;
        }
    }

    return "";
}

void Schema::escapeStr(std::string &source)
{
    Utils::str_replace(source, "'", "''");
}

string Schema::parseAndStore(
        AutoSqlite *pAutoSqlite,
        string &parse_data, vector<string> &parent_ano,
        string &parseTime,
        const char *parentId, const char *childIdMap)
{
    Value root;
    Reader reader;

    if (reader.parse(parse_data, root, false))
    {
        return parseAndStore(pAutoSqlite, root, root, parseTime, parent_ano, parentId, childIdMap);
    }
    else
    {
        LOG_ERROR(LOG_GLOBAL, "[Schema::parseAndStore] Error parsing %s", mTable.c_str());
        return "";
    }
}

string Schema::parseAndStore(
        AutoSqlite *pAutoSqlite,
        const Value &rootValue, const Value &values,
        string &parseTime,
        vector<string> &parent_ano, const char *parentId, const char *childIdMap)
{
    //LOG_INFO(LOG_SCHEMA, "[Schema::parseAndStore] %s", mTable.c_str());

    field_map data;
    getValues(data, rootValue, values, parseTime, parent_ano, parentId, childIdMap);
    return store(pAutoSqlite, data);
}

string Schema::store(AutoSqlite *pAutoSqlite, field_map &data, bool bPending)
{
    return store(pAutoSqlite, data, nullptr, bPending);
}

string Schema::store(AutoSqlite *pAutoSqlite, field_map &data, const char *where, bool bPending)
{
    string id = data["id"].get();

    //LOG_INFO(LOG_SCHEMA, "[Schema::store] %s - %s", mTable.c_str(), id.c_str());

    std::ostringstream dest;
    std::ostringstream whereStm;
    bool bSuccess = false;

    if (nullptr == where)
    {
        whereStm << "id=" << id;
    }
    else
    {
        whereStm << where;
    }

    string whereStr = whereStm.str();

    if (nullptr != pAutoSqlite && pAutoSqlite->isOpen())
    {
        bSuccess = true;
        switch (pAutoSqlite->query(mTable.c_str(), whereStr, data))
        {
            case ITEM_NEW:
                getInsert(dest, data);
                break;

            case ITEM_UNMODIFIED:
                break;

            case ITEM_MODIFIED:
                getUpdate(dest, data);
                break;
        }

        if (!bPending)
        {
            //std::cout << "[Schema::store] " << mTable << ":" << id << "->" << dest.str() << std::endl;
            if (pAutoSqlite->exec(dest.str()))
            {
                notifyTableChanged(id.c_str());
            }
        }
        else
        {
            string execStr = dest.str();
            ORMSQLite::getInstance()->pushExecPending(getDbType(), execStr);
        }
    }
    else
    {
        std::string dbPath = "";

        for (field_map::iterator iter = data.begin(); iter != data.end(); ++iter)
        {
            if (iter->second.isDbPath())
            {
                dbPath = iter->second.getRaw();
                break;
            }
        }

        AutoSqlite sqlite(ORMSQLite::getInstance()->getDb(this, dbPath));
        if (sqlite.isOpen())
        {
            bSuccess = true;

            //LOG_INFO(LOG_SCHEMA, "[Schema::store] %s WHERE %s", mTable.c_str(), where.c_str());
            //printValues(mTable.c_str(), data);

            switch (sqlite.query(mTable.c_str(), whereStr, data))
            {
                case ITEM_NEW:
                    getInsert(dest, data);
                    break;

                case ITEM_UNMODIFIED:
                    break;

                case ITEM_MODIFIED:
                    getUpdate(dest, data);
                    break;
            }

            if (!bPending)
            {
                if (sqlite.exec(dest.str()))
                {
                    notifyTableChanged(id.c_str());
                }
            }
            else
            {
                string execStr = dest.str();
                ORMSQLite::getInstance()->pushExecPending(getDbType(), execStr);
            }
        }
    }
    if (!bSuccess)
    {
        std::ostringstream dest;
        setUpdateNeeded(data);
        getUpsert(dest, whereStr.c_str(), data);
        string pending = dest.str();
        ORMSQLite::getInstance()->pushExecPending(getDbType(), pending);
    }

    Utils::stripQuotes(id);
    return id;
}

void Schema::storeNew(AutoSqlite *pAutoSqlite, field_map &data)
{
    string id = data["id"].get();

    //LOG_INFO(LOG_SCHEMA, "[Schema::storeNew] %s", mTable.c_str());

    std::ostringstream dest;
    std::ostringstream whereStm;

    if (nullptr != pAutoSqlite && pAutoSqlite->isOpen())
    {
        getInsert(dest, data);

        if (pAutoSqlite->exec(dest.str()))
        {
            notifyTableChanged(id.c_str());
        }
    }
    else
    {
        AutoSqlite sqlite(ORMSQLite::getInstance()->getDb(this));
        if (sqlite.isOpen())
        {
            getInsert(dest, data);

            if (sqlite.exec(dest.str()))
            {
                notifyTableChanged(id.c_str());
            }
        }
    }
}

void Schema::initParents()
{
    size_t count = mCols.size();
    for (size_t i = 0; i < count; i++)
    {
        string type = mTypes[i];
        vector<string> &ano = mAnos[i];

        bool bFromParent = getAnnotationBool("fromParent=", ano);

        if (!bFromParent)
        {
            if (0 == type.compare(0, mModelPrefix.length(), mModelPrefix))
            {
                string modelType = type.substr(mModelPrefix.length());
                Schema *pSchema = ORMSQLite::getInstance()->getSchema(modelType);
                if (nullptr != pSchema)
                {
                    pSchema->addParent(this);
                }
            }
            else if (0 == type.compare(0, mModelListPrefix.length(), mModelListPrefix))
            {
                string modelType = type.substr(mModelListPrefix.length());
                Schema *pSchema = ORMSQLite::getInstance()->getSchema(modelType);
                if (nullptr != pSchema)
                {
                    pSchema->addParent(this);
                }
            }
        }
    }
}

void Schema::addParent(Schema *pSchema)
{
    //LOG_INFO(LOG_SCHEMA, "[Schema::initParents] %s add parent model: %s", mTable.c_str(), pSchema->getTable());
    mParents.push_back(pSchema);
}

void Schema::notifyTableChanged(const char *id)
{
    std::string itemId(id);
    Utils::stripQuotes(itemId);
    ORMSQLite::getInstance()->notifyTableChanged(mTable.c_str(), itemId.c_str());
    size_t count = mParents.size();
    for (size_t i = 0; i < count; i++)
    {
        mParents[i]->notifyTableChanged(nullptr);
    }
}

//TODO: handle miscData
void Schema::serialize(Json::Value& json, field_map& data, std::vector<std::string>& fields)
{
    for (std::string field : fields)
    {
        field_map::iterator iter = data.find(field);
        if (iter != data.end())
        {
            switch (iter->second.getType())
            {
                case FieldType::BOOL:
                    json[field] = Json::Value(iter->second.getInt()>0?true:false);
                    break;

                case FieldType::LONG:
                    json[field] = (UInt64)iter->second.getUint64();
                    break;

                case FieldType::INT:
                    json[field] = iter->second.getInt();
                    break;

                case FieldType::FLOAT:
                    json[field] = iter->second.getFloat();
                    break;

                case FieldType::DOUBLE:
                    json[field] = iter->second.getDouble();
                    break;

                case FieldType::ENUM:
                    json[field] = iter->second.getRaw();
                    break;

                case FieldType::STRING:
                    json[field] = iter->second.getRaw();
                    break;

                case FieldType::B91JSON:
                {
                    Json::Reader reader;
                    Json::Value jsonResult;
                    reader.parse(iter->second.getRaw(), jsonResult, false);
                    if (jsonResult.empty())
                    {
                        jsonResult = Json::objectValue;
                    }
                    json[field] = jsonResult;
                }
                break;
            }
        }
    }
}

void Schema::createTable(AutoSqlite *pAutoSqlite)
{
    std::ostringstream sql;
    int ix = 0;
    sql << "CREATE TABLE IF NOT EXISTS " << mTable << " (";
    for (std::string col : mCols)
    {
        if (ix > 0)
        {
            sql << ",";
        }
        sql << col << " ";

        // Handles the basic types
        std::string type = mTypes[ix];

        if (type == "string")
            sql << "BLOB";
        else if (type == "b91json")
            sql << "BLOB";
        else if (type == "long")
            sql << "INTEGER";
        else if (type == "int")
            sql << "INTEGER";
        else if (type == "bool")
            sql << "INTEGER";
        else if (type == "double")
            sql << "REAL";
        else if (type == "float")
            sql << "REAL";

        vector<string> ano = mAnos[ix];

        string appDb = getAnnotation("appDb=", ano);
        if (!appDb.empty())
        {
            sql << " " << appDb;
        }

        ix++;
    }
    sql << ");";

    //LOG_INFO(LOG_SCHEMA, "[Schema::createTable] %s", sql.str().c_str());

    pAutoSqlite->exec(sql.str());
}
