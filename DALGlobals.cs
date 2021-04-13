using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using System.Configuration;

namespace DataAccess
{
    public static class DALGlobals
    {
        public static string KEY_USER_ID = "__uid__";
        public static string KEY_ACTION = "__action__";
        public static string KEY_TEMP_ID = "__temp_id__";
        public static string KEY_NEW_TEMP_ID = "_newId";
        public static string KEY_INDICES = "__key_indices__";
        public static string KEY_TABLE_CODES = "__code__";
        public static string KEY_LINK_CODES = "__links__";
        public static string KEY_NEWREC_TEMP_ID = "__temp_record_id__";

        public static string LNK_OTO = "1to1";
        public static string LNK_OTM = "1tom";
        public static string LNK_LKP = "lookup";
        public static string LNK_NO_STAMP = "nostamp";


        public static string SQLT_SELECT_ALL = "select {0} from {1}";

        public static string SQLT_UPDATE = "update {0} set {1} where ({2});";
        public static string SQLT_INSERT = "insert into {0} ( {1} ) SELECT {2};";
        public static string SQLT_DELETE = "delete from {0} where ({1});";


        public static string SQLT_PARAM_KEY = DALData.PARAM_PREFIX + "p{0}";
        public static string SQLT_FIELD = "[{0}]";
        public static string SQLT_SET = "[{0}] = "+ DALData.PARAM_PREFIX + "p{1}";
        public static string SQLT_SELECT_AS = DALData.PARAM_PREFIX + "p{0} as [{1}]";

        public static string RES_NO_ACTION = "no action";
        public static string RES_SUCCESS = "success";
        public static string RES_ERROR = "ERROR";

        public static string DAT_FOR_UPDATE = "forUpdate";
        public static string DAT_FOR_NEW = "forNew";
        public static string DAT_FOR_DELETE = "forDelete";

        public static string DATA_PARAMS = "dataParams";

        public static char PARAMS_DELIM_CHAR = '`';
        public static char PARAMS_DELIM_CHAR_CAT = '|';
        
        public static char PARAMS_VAL_DELIM_CHAR = ',';
        public static string FIELD_PARENT_LINK_ALIAS = "lnk_id";
        public static string FIELD_CHILD_COUNT_ALIAS = "lnk_child_count";
        public static string FIELD_CHILD_FIRST_ALIAS = "lnk_child_first";
        public static string FIELD_CHILD_MAX_ALIAS = "lnk_child_max";

        static DALGlobals()
        {
            //// Initialize data access connection and other properties
            //// DAL.connectionString = ConfigurationManager.ConnectionStrings["cnsAppAPI"].ConnectionString;
            //DALGlobals.APP_SETTINGS = DataAccess.AppGlobals2.AppSetings;
            //DALData.DAL.connectionString = ConfigurationManager.ConnectionStrings[DALGlobals.APP_SETTINGS["CONNECTION_NAME"]].ConnectionString;
            //DALGlobals.GeneralRetObj = new ReturnObjectExternal();
            //AppDataset.configPath = "";
            //AppDataset.clientDevPath = "";

            //DALData.DAL.LogMessage("Application Started ..");
            //DALData.DAL.LogMessage("Schema Path: " + DataAccess.AppGlobals2.PATH_SCHEMA_CONFIG);
            //DALData.DAL.LogMessage("Client Tables Path: " + DataAccess.AppGlobals2.PATH_TARGET_TYPESCRIPT_PATH);
            //DALData.DAL.LogMessage(HttpContext.Current.Server.MapPath("App_Data"));

            //// Initialize dataset
            //AppDataset.Initialize();
        }


        static public Dictionary<string, dynamic> APP_SETTINGS { set; get; }

        static public ReturnObjectExternal GeneralRetObj { set; get; }

        static public string appTypeScriptFile(string scriptFile = "app.tables.ts")
        {
            return APP_SETTINGS["PATH_TARGET_TYPESCRIPT_FOLDER"] != "" ?
                APP_SETTINGS["PATH_TARGET_TYPESCRIPT_FOLDER"] :
                APP_SETTINGS["PATH_SCHEMA_CLIENT"] + "\\" + scriptFile;
        }

        public static string BlnToStr(bool value)
        {
            return Convert.ToString(value).ToLower();
        }

        public static string GetDelim(int index, string delimiter = ", ")
        {
            return GetDelim(index == 0, delimiter);
        }

        //public static DateTime DateTimeNow
        //{
        //    get
        //    {
        //        return Convert.ToDateTime(DateTime.Now.ToString("MMMM dd, yyyy HH:mm:ss"));
        //    }
        //}


        public static string btoa(string encStr)
        {
            // decode base64 string
            return System.Text.Encoding.Default.GetString(Convert.FromBase64String(encStr));   
        }

        public static string atob(string str)
        {
            //byte[] bytes = Convert.FromBase64String(str);                 convert to byte array
            //string jsonText = System.Text.Encoding.Default.GetString(jsonBytes);// convert to JSON string
            //JObject json = JObject.Parse(jsonText);

            // encode string to base64
            return Convert.ToBase64String(System.Text.Encoding.Default.GetBytes(str));
        }

        public static JArray btoJA(string encStr)
        {
            // converts base64 to ascii version of the json object
            string jString = btoa(encStr);
            if (jString.TrimStart(' ').IndexOf("[") == 0)
            {
                // if jstring starts with a square bracket, it means that the input object is a JArray
                return JArray.Parse(jString);
            }
            else
            {
                // else the input object is a JObject and will become the sole member of the return JArray object
                return new JArray() { JObject.Parse(jString) };
            }
        }

        public static JObject btoJO(string encStr)
        {
            //encStr = encStr.TrimStart(' ');
            //("    [ ]").TrimStart(' ').IndexOf("#")

            string jString = btoa(encStr);

            if (jString.TrimStart(' ').IndexOf("[") == 0)
            {
                return JObject.Parse("{\"jarray\":" +  jString + "}");
            }
            else
            {
                return JObject.Parse(jString);
            }
            
        }

        public static dynamic atoJD(string encStr)
        {
            JObject obj = JObject.Parse("{\"par\":"+ "" +"}");
            return null;
        }

        public static string GetDelim(string currentValue, string delimiter = ", ")
        {
            return GetDelim(currentValue.Length == 0, delimiter);
        }
        public static string GetDelim(bool initial, string delimiter = ", ")
        {
            return (initial ? "" : delimiter);
        }

        /*************************************************************************************************/
        public static dynamic TKVDyn(JObject jObject, string key, string toType = "string", dynamic defValue = null)
        {
            if (defValue == null) defValue = Convert.DBNull;

            if (jObject.ContainsKey(key))
            {
                return DALData.DAL.DALCastValue(toType, jObject.GetValue(key));
            }
            else
            {
                return defValue;
            }
        }

        public static JProperty GetJPropery(JObject jObject, string name)
        {
            foreach (JProperty jp in jObject.Properties())
            {
                if (jp.Name == name) return jp;
            }
            return null;
        }

        public static string[] Split(string source, string delimiter)
        {
            string[] stringSeparators = new string[] { delimiter };
            return source.Split(stringSeparators, StringSplitOptions.None);
        }

        public static Int64 TKV64(JObject jObject, string key, Int64 defValue = -1)
        {
            if (jObject.ContainsKey(key))
            {
                return Convert.ToInt64(jObject.GetValue(key));
            }
            else
            {
                return defValue;
            }
        }



        public static int TKVInt(JObject jObject, string key, int defValue = -1)
        {
            if (jObject.ContainsKey(key))
            {
                return Convert.ToInt32(jObject.GetValue(key));
            }
            else
            {
                return defValue;
            }
        }

        public static bool TKVBln(JObject jObject, string key, bool defValue = false)
        {
            if (jObject.ContainsKey(key))
            {
                return Convert.ToBoolean(jObject.GetValue(key).ToString().ToLower());
            }
            else
            {
                return defValue;
            }
        }
        public static string TKVStr(JObject jObject, string key, string defValue = "")
        {
            if (jObject.ContainsKey(key))
            {
                return Convert.ToString(jObject.GetValue(key));
            }
            else
            {
                return defValue;
            }
        }

        public static double TKVDbl(JObject jObject, string key, double defValue = 0.0)
        {
            if (jObject.ContainsKey(key))
            {
                return Convert.ToDouble(jObject.GetValue(key));
            }
            else
            {
                return defValue;
            }
        }

        public static JArray TKVJArr(JObject jObject, string key, JArray defValue = null)
        {
            if (jObject.ContainsKey(key))
            {
                dynamic tmp = jObject.GetValue(key);
                return tmp;

            }
            else
            {
                return defValue != null ? defValue : new JArray();
            }
        }


        public static DateTime DateTimeNow
        {
            get
            {
                return Convert.ToDateTime(DateTime.Now.ToString("MMMM dd, yyyy HH:mm:ss"));
            }
        }

        public static List<CommandParam> MergeCommandParams(List<CommandParam> master, CommandParam additionalParam)
        {

            List<CommandParam> retVal = master;

            if (retVal == null) retVal = new List<CommandParam>();

            retVal.Add(additionalParam);
            return retVal;
        }
        public static List<CommandParam> MergeCommandParams(List<CommandParam> master, List<CommandParam> additionalParams)
        {
            List<CommandParam> retVal = master;
            if (retVal == null) retVal = new List<CommandParam>();

            foreach (CommandParam cmd in additionalParams) retVal.Add(cmd);

            return retVal;
        }

        public static Dictionary<string, dynamic> MergeParams(Dictionary<string, dynamic> master,
            ColumnInfo col, JProperty jp)
        {
            return MergeParams(master, DALData.DAL.DALCastValue(col.type, jp.Value));
        }
        public static Dictionary<string, dynamic> MergeParams(Dictionary<string, dynamic> master, 
            ColumnInfo col, JToken rec)
        {
            return MergeParams(master, DALData.DAL.DALCastValue(col.type, rec[col.name]));
        }
        public static Dictionary<string,dynamic> MergeParams(Dictionary<string,dynamic> master,dynamic value)
        {
            Dictionary<string, dynamic> retVal = master;

            if (retVal == null) retVal = new Dictionary<string, dynamic>();
            retVal.Add(DALData.PARAM_PREFIX + "p" + retVal.Count, value);

            return retVal;
        }

        public static void MergeParams(dynamic value, Dictionary<string, dynamic> master)
        {
            if (master == null) return;
            master.Add(DALData.PARAM_PREFIX + "p" + master.Count, value);
        }


    }

    public static class AppGlobals2
    {
        public static string KEY_REQUEST_HEADER_CODE = "__header__";
        public static string KEY_PROCESS_HEADER_CODE = "__process__";
        public static string KEY_PROCESS_CONFIG_CODE = "__config__";

        public static string KEY_REQUEST_STAMP = "_req_stamp_";
        public static string KEY_USER_ID = "__uid__";
        public static string KEY_USER_RIGHTS = "__rights__";
        public static string KEY_ACTION = "__action__";
        public static string KEY_TEMP_ID = "__temp_id__";
        public static string KEY_NEW_TEMP_ID = "_newId";
        public static string KEY_INDICES = "__key_indices__";
        public static string KEY_TABLE_CODES = "__code__";
        public static string KEY_NEWREC_TEMP_ID = "__temp_record_id__";

        public static string KEY_QPARAM_JSON = "_p";
        public static string KEY_REQ_ARGS_ARR = "_requestArgs";
        


        public static string KEY_TABLE_UPDATE_TRACK_CODE = "chgTrack";


        public static string KEY_CONTENT_TYPE = "contentType";


        public static string QS_SUBSCRIPTION_KEY = "skey";

        public static string LNK_1_TO_1 = "1to1";


        public static string RES_NO_ACTION = "no action";

        public static string CONFIG_FILE= "";

        //Properties.Settings.
        public static string FMT_TABLE_CONFIG = AppSet("FMT_TABLE_CONFIG");
        public static string FMT_VIEW_CONFIG = AppSet("FMT_VIEW_CONFIG");
        public static string FMT_PROCEDURE_CONFIG = AppSet("FMT_PROCEDURE_CONFIG");

        public static string FMT_TABLE_MODEL = "{0}.model.json";

        private static JObject _JSettings = null;

        public static void ReadJSettings(string cfgFile)
        {
            _JSettings = JObject.Parse(File.ReadAllText(APP_PATH + "\\" + cfgFile));
        }
        public static JObject JSettings
        {
            get
            {

                //return _JSettings;

                if (_JSettings != null) return _JSettings;

                //string cfgPath = CONFIG_FILE;

                _JSettings = JObject.Parse(File.ReadAllText(APP_PATH + "\\app.settings.json"));
                //_JSettings = JObject.Parse(File.ReadAllText(APP_PATH + "\\" + cfgPath));

                return _JSettings;
            }
        }

        private static Dictionary<string, dynamic> _AppSettings = null;
        public static Dictionary<string, dynamic> AppSetings
        {
            get
            {
                if (_AppSettings != null) return _AppSettings;

                _AppSettings = new Dictionary<string, dynamic>() { };

                foreach (KeyValuePair<string, JToken> tk in JSettings)
                {
                    _AppSettings.Add(tk.Key, Convert.ToString(tk.Value));
                }

                // override asppsettings with local properties
                _AppSettings["PATH_SETTINGS"] = PATH_SETTINGS;
                _AppSettings["PATH_SCHEMA"] = PATH_SCHEMA;
                _AppSettings["PATH_SCHEMA_CONFIG"] = PATH_SCHEMA_CONFIG;
                _AppSettings["PATH_SCHEMA_CLIENT"] = PATH_SCHEMA_CLIENT;
                _AppSettings["PATH_SCHEMA_TEMPLATES"] = PATH_SCHEMA_TEMPLATES;

                return _AppSettings;
            }
        }

        public static string PTN_TABLE_CONFIG
        {
            get { return String.Format(FMT_TABLE_CONFIG, "*"); }
        }
        public static string PTN_VIEW_CONFIG
        {
            get { return String.Format(FMT_VIEW_CONFIG, "*"); }
        }
        public static string PTN_PROCEDURE_CONFIG
        {
            get { return String.Format(FMT_PROCEDURE_CONFIG, "*"); }
        }

        public static string APP_PATH
        {
            // returns the host project application's path instead of the class library path
            get { return AppDomain.CurrentDomain.BaseDirectory; }
        }

        public static string PATH_SETTINGS
        {
            get { return APP_PATH + AppSet("PATH_SETTINGS"); }
        }

        public static string PATH_SCHEMA
        {
            get
            {
                string ret = PATH_SETTINGS + "\\" + AppSet("PATH_SCHEMA");
                if (!Directory.Exists(ret)) Directory.CreateDirectory(ret);
                return ret;
            }
        }
        public static string PATH_SCHEMA_CONFIG
        {
            get
            {

                string ret = PATH_SCHEMA + "\\" + AppSet("PATH_SCHEMA_CONFIG");
                if (!Directory.Exists(ret)) Directory.CreateDirectory(ret);
                return ret;
            }
        }
        public static string PATH_TARGET_TYPESCRIPT_PATH
        {
            get
            {

                string ret = AppSet("PATH_TARGET_TYPESCRIPT_PATH");
                //if (!Directory.Exists(ret)) Directory.CreateDirectory(ret);
                return ret;
            }
        }
        public static string TPL_TARGET_TYPESCRIPT_IMPORT
        {
            get
            {

                string ret = AppSet("TPL_TARGET_TYPESCRIPT_IMPORT");
                return ret;
            }
        }
        public static string TPL_TARGET_TYPESCRIPT_INSTANCE
        {
            get
            {

                string ret = AppSet("TPL_TARGET_TYPESCRIPT_INSTANCE");
                return ret;
            }
        }

        public static string PATH_TARGET_TYPESCRIPT_DATASET
        {
            get
            {

                string ret = AppSet("PATH_TARGET_TYPESCRIPT_DATASET");
                //if (!Directory.Exists(ret)) Directory.CreateDirectory(ret);
                return ret;
            }
        }
        //
        public static string PATH_SCHEMA_CLIENT
        {
            get
            {
                string ret = PATH_SCHEMA + "\\" + AppSet("PATH_SCHEMA_CLIENT");
                if (!Directory.Exists(ret)) Directory.CreateDirectory(ret);
                return ret;
            }
        }

        public static string appTypeScriptFile(string scriptFile = "app.tables.ts")
        {
            return AppSet("PATH_TARGET_TYPESCRIPT_FOLDER") != "" ?
                AppSet("PATH_TARGET_TYPESCRIPT_FOLDER") :
                AppSet("PATH_SCHEMA_CLIENT") + "\\" + scriptFile;
        }


        public static string PATH_SCHEMA_TEMPLATES
        {
            get
            {
                string ret = PATH_SCHEMA + "\\" + AppSet("PATH_SCHEMA_TEMPLATES");
                if (!Directory.Exists(ret)) Directory.CreateDirectory(ret);
                return ret;
            }
        }
        public static dynamic AppSet(string key)
        {
            dynamic ret = JSettings.GetValue(key);
            return ret == null ? "No Value" : ret;
        }

        public static string[] Split(string source, string delimiter)
        {
            string[] stringSeparators = new string[] { delimiter };
            return source.Split(stringSeparators, StringSplitOptions.None);
        }

        public static string ConfigTableFile(string configCode = "*", bool withPath = false)
        {
            return (withPath ? PATH_SCHEMA_CONFIG : "") + String.Format(FMT_TABLE_CONFIG, configCode);
        }

        public static string ConfigViewFile(string configCode = "*", bool withPath = false)
        {
            return (withPath ? PATH_SCHEMA_CONFIG : "") + String.Format(FMT_VIEW_CONFIG, configCode);
        }

        public static string ConfigProcedureFile(string configCode = "*", bool withPath = false)
        {
            return (withPath ? PATH_SCHEMA_CONFIG : "") + String.Format(FMT_PROCEDURE_CONFIG, configCode); ;
        }

        public static string GetCodeFromPattern(string filename, string pattern)
        {
            int marker = pattern.IndexOf("*");
            string prefix = pattern.Substring(0, marker);
            string suffix = pattern.Substring(marker + 1);

            int fileMarker = filename.IndexOf(prefix);

            return filename.Substring(fileMarker + prefix.Length, filename.Length - pattern.Length - fileMarker + 1);
            //return filename + " , " +  pattern;
        }

        public static dynamic GetTokenValue(JObject jObject, string key, dynamic defValue = null)
        {
            if (jObject.ContainsKey(key))
            {
                return jObject.GetValue(key).ToString();
            }
            else
            {
                return defValue;
            }
        }

        public static JProperty GetJPropery(JObject jObject, string name)
        {
            foreach (JProperty jp in jObject.Properties())
            {
                if (jp.Name == name) return jp;
            }
            return null;
        }

        public static Int64 TKV64(JObject jObject, string key, Int64 defValue = -1)
        {
            if (jObject.ContainsKey(key))
            {
                return Convert.ToInt64(jObject.GetValue(key));
            }
            else
            {
                return defValue;
            }
        }

        public static int TKVInt(JObject jObject, string key, int defValue = -1)
        {
            if (jObject.ContainsKey(key))
            {
                return Convert.ToInt32(jObject.GetValue(key));
            }
            else
            {
                return defValue;
            }
        }

        public static bool TKVBln(JObject jObject, string key, bool defValue = false)
        {
            if (jObject.ContainsKey(key))
            {
                return Convert.ToBoolean(jObject.GetValue(key).ToString().ToLower());
            }
            else
            {
                return defValue;
            }
        }
        public static string TKVStr(JObject jObject, string key, string defValue = "")
        {
            if (jObject.ContainsKey(key))
            {
                return Convert.ToString(jObject.GetValue(key));
            }
            else
            {
                return defValue;
            }
        }

        public static List<string> TKVStrArr(JObject jObject, string key, List<string> defValue = null)
        {
            if (jObject.ContainsKey(key))
            {
                List<string> ret = new List<string>();
                foreach (string js in jObject.GetValue(key))
                {
                    ret.Add(js);
                }
                return ret;
            }
            else
            {
                return defValue;
            }
        }
        public static double TKVDbl(JObject jObject, string key, double defValue = 0.0)
        {
            if (jObject.ContainsKey(key))
            {
                return Convert.ToDouble(jObject.GetValue(key));
            }
            else
            {
                return defValue;
            }
        }

        public static JObject TKVJObj(JObject jObject, string key, JObject defValue = null)
        {
            if (jObject.ContainsKey(key))
            {
                dynamic tmp = jObject.GetValue(key);
                return tmp;

            }
            else
            {
                return defValue != null ? defValue : new JObject();
            }
        }
        public static JArray TKVJArr(JObject jObject, string key, JArray defValue = null)
        {
            if (jObject.ContainsKey(key))
            {
                dynamic tmp = jObject.GetValue(key);
                return tmp;

            }
            else
            {
                return defValue != null ? defValue : new JArray();
            }
        }

        public static string C2S(char c)
        {
            return Convert.ToString(c);
        }

    }

    // enums
    public static class SQLModes
    {
        public const string
            SORT = "sort",
            CONDITION = "cond",
            GROUP = "group",
            FIELDS = "fields",
            SELECT = "select",
            INSERT = "insert",
            UPDATE = "update",
            DELETE = "delete",
            LINK = "link";
    }

    public static class SQLJoinChars
    {
        public const char
            INNER_JOIN_SYMBOL = '-',
            LEFT_JOIN_SYMBOL = '`',
            LINK_JOIN_SYMBOL = '^',
            LINK_LEFT_FILTER_SYMBOL = '<',
            LINK_RIGHT_FILTER_SYMBOL = '>',
            TABLE_CODE_SEPARATOR = '|',
            PIPE_SEPARATOR = '|',
            OPERATOR_PIPE = '|',
            JOIN_SEPARATOR = ';',
            WHERE_START_SEPARATOR = '{',
            WHERE_END_SEPARATOR = '}',
            CARET_SEPARATOR = '^',
            TILDE_SEPARATOR = '~',
            AGGREGATE_SEPARATOR = '(',
            ARGUMENTS_SEPARATOR = ',',
            ALIAS_SEPARATOR = '@',      // table or field alias symbol
            ALIAS_EXPRESSION = '.',     // table alias expression when specified before a fieldname
            FIELD_SEPARATOR = '`';
    }

    public static class SQLJoinCStr
    {
        public const string
            INNER_JOIN_SYMBOL = "-",
            LEFT_JOIN_SYMBOL = "`",
            LINK_JOIN_SYMBOL = "^",
            LINK_LEFT_FILTER_SYMBOL = "<",
            LINK_RIGHT_FILTER_SYMBOL = ">",
            TABLE_CODE_SEPARATOR = "|",
            PIPE_SEPARATOR = "|",
            OPERATOR_PIPE = "|",
            JOIN_SEPARATOR = ";",
            WHERE_START_SEPARATOR = "{",
            WHERE_END_SEPARATOR = "}",
            CARET_SEPARATOR = "^",
            TILDE_SEPARATOR = "~",
            AGGREGATE_SEPARATOR = "(",
            ARGUMENTS_SEPARATOR = ",",
            ALIAS_SEPARATOR = "@",      // table or field alias symbol
            ALIAS_EXPRESSION = ".",     // table alias expression when specified before a fieldname
            FIELD_SEPARATOR = "`";
    }

    public static class TrackingActions
    {
        public const string
            UPDATE = "U",
            DELETE = "D",
            CREATE = "C";
    }

    public static class TableRelationTypes
    {
        public const string
            LINK = "lnk",               // intermediate link table between two tables
            LOOKUP = "lkp",             // link to a lookup table
            LOOKUP_GROUP = "lkpg",      // link to a composite lookup table with group filtering
            PARENT = "par",             // parent table is directly linked to the child table
            ONE2MANY = "1tom",          // child table is directly linked to the parent table (one to many)
            ONE2ONE = "1to1";              // child table is directly linked to the parent table (one to one)
    }
    public static class TableRelationActions
    {
        public const string
            ADD = "add",
            REMOVE = "remove";
    }

}
