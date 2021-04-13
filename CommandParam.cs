using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace DataAccess
{
    public class CommandParam
    {

        public CommandParam(string _cmdText = null,
                Dictionary<string, dynamic> _cmdParams = null,
                string _cmdGroup = null, Int64 _tempKey = 0, Int64 _newKey = -1,
                string _tempStrKey = null, string _newStrKey = null, DALTable _table = null, JObject _cmdOutput = null, Int64 _paramKeyValuePosition = -1, Int64 _paramKeyValue= -1)
        {

            CommandParamInit(_cmdText, _cmdParams, _cmdGroup, _tempKey, _newKey, _tempStrKey, _newStrKey, _table, _cmdOutput, _paramKeyValuePosition, _paramKeyValue);
        }

        public CommandParam(string _cmdText, List<dynamic> paramArray=null)
        {
            Dictionary<string, dynamic> cmdParamsObj = null;
            if (paramArray != null)
            {
                cmdParamsObj = new Dictionary<string, dynamic>();
                for (int idx=0;idx<paramArray.Count;idx++)
                    cmdParamsObj.Add(DALData.PARAM_PREFIX + "p" + idx.ToString(), paramArray.ElementAt(idx));
            }
            CommandParamInit(_cmdText, cmdParamsObj);
        }

        private void CommandParamInit(string _cmdText = null, 
                Dictionary<string, dynamic> _cmdParams = null, 
                string _cmdGroup=null,Int64  _tempKey=0, Int64 _newKey=-1, 
                string _tempStrKey= null, string _newStrKey = null, DALTable _table = null, JObject _cmdOutput = null, Int64 _paramKeyValuePosition = -1, Int64 _paramKeyValue = -1)
        {
            //
            // TODO: Add constructor logic here
            //

            _SQLMode = (this.newKey != 0 || this.newStrKey != null ? SQLModes.INSERT : "");

            if (_cmdText != null) cmdText = _cmdText;
            if (_cmdParams != null) cmdParams = _cmdParams;
            if (_cmdGroup != null) cmdGroup = _cmdGroup;
            if (_table != null) table = _table;

            if (_cmdOutput != null) cmdOutput = _cmdOutput;

            tempKey = _tempKey;
            newKey = _newKey;

            tempStrKey = _tempStrKey;
            newStrKey = _newStrKey;

            paramKeyValuePosition = _paramKeyValuePosition;
            paramKeyValue = _paramKeyValue;

            //if (_cmdInput != null) cmdInput = _cmdInput;

        }

        //public List<ColumnInfo> tmpCols;
        //public List<ColumnInfo> tmpKeys;


        public JObject cmdInput { set; get; }
        public JObject cmdOutput { set; get; }
        public String __cmdText;
        public String cmdText
        {
            set
            {
                __cmdText = value;
                if (SQLMode == "")
                {
                    if (__cmdText.IndexOf("delete ") != 0) _SQLMode = SQLModes.DELETE;
                    if (__cmdText.IndexOf("update ") != 0) _SQLMode = SQLModes.UPDATE;
                }
            }
            get
            {
                return __cmdText;
            }
        }

        public Int64 tempKey { get; set; }
        public Int64 newKey { get; set; }

        public string tempStrKey { get; set; }
        public string newStrKey { get; set; }
        public DALTable table { get; set; }

        private string _SQLMode;
        public string SQLMode { get { return _SQLMode; } } 

        // cmdGroup property is to be used to group the result output. 
        // normally the output data to report are 
        // - the number of records processed
        // - if error occured during processing
        // - how long the process took place
        // - <future report parameter(s)>
        public String cmdGroup { set; get; }    

        public Dictionary<string, dynamic> cmdParams { set; get; }
        public Int64 paramKeyValuePosition { set; get; }
        public Int64 paramKeyValue { set; get; }

    }

}
