using System;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;


namespace DataAccess
{
    public class DALView
    {
        public DALView(string viewName = "", string description = "",
            FieldInfo[] viewParams = null, string viewCode = "")
        {
            this.viewName = viewName;
            this.description = description;
            this.viewParams = viewParams;
            this.viewCode = viewCode;
        }

        public string viewName { set; get; }
        public string viewCode { set; get; }
        public string description { set; get; }

        // parameter type field list definition must be in the same order  
        // as parameters are defined in the stored procedure
        public FieldInfo[] viewParams { set; get; }

        public ReturnObject Get(JObject args = null,int objOrder=-1)
        {
            // args - combined parameters from querystring and internally generated parameters
            // objOrder - argument set ordinal value

            JObject cmdArgs;
            if (objOrder != -1)
            {
                cmdArgs = new JObject();

            }
            else
            {
                cmdArgs = args;
            }

            Dictionary<string, dynamic> prms = DALData.DAL.DALBuildParams(viewParams, cmdArgs);
            ReturnObject ret = DALData.DAL.GetRecordset(new CommandParam()
            {
                cmdText = viewName,
                cmdParams = prms
            });


            return ret;
        }


    }
}
