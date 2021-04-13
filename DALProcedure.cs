using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using _g = DataAccess.DALGlobals;
using Newtonsoft.Json.Linq;


namespace DataAccess
{
    public class DALProcedure
    {
        public DALProcedure(string procName = "", string description = "",
            FieldInfo[] procParams = null)
        {
            this.procName = procName;
            this.description = description;
            this.procParams = procParams;
        }


        public string procName { set; get; }
        public string description { set; get; }

        // parameter type field list definition must be in the same order  
        // as parameters are defined in the stored procedure
        public FieldInfo[] procParams { set; get; }

    }
}
