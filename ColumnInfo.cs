using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataAccess
{
    public class FieldInfo
    {
        public FieldInfo(string name="", 
            string type = "String",
            string caption = "",
            string alias="",
            string roles="",
            bool isParameter=true,
            bool isLong=false) {

            this.name = name;
            this.type = type;
            this.caption = caption;
            this.alias = alias;
            this.roles= roles;
            this.isLong = isLong;

            this.log = "";

            this.isParameter = isParameter;
        }
        public bool isParameter { get; set; }
        public bool isLong { get; set; }
        public double size { get; set; }
        public string name { get; set; }
        public string alias { get; set; }
        public string roles { get; set; }
        public string type { get; set; }
        public string caption { get; set; }
        public string log { get; set; }

    }

    public class ColumnInfo:FieldInfo
    {
        public ColumnInfo(
            string name= "",
            string type = "String",
            string caption = "",
            string alias = "",
            string roles = "",
            int keyPosition=-1,
            int uniquePosition=-1,
            int groupPosition=-1,
            int sortPosition=-1,
            int displayPosition=-1,
            bool isRequired=false,
            bool isLong=false,
            string prefix =""
            )
        {

            this.name = name;//.ToLower();
            this.type = type;
            this.caption = caption;
            this.alias = alias;
            this.roles = roles;

            this.keyPosition = keyPosition;
            this.uniquePosition = uniquePosition;
            this.groupPosition = groupPosition;
            this.sortPosition = sortPosition;
            this.displayPosition = displayPosition;

            this.prefix = prefix;
            this.isRequired = isRequired;
            this.isLong = isLong;

            this.maxLength = -1;

            this.isParameter = false;

        }

        public DALTable table { get; set; }

        public string prefix { get; set; }

        public int maxLength { get; set; }

        public int uniquePosition { get; set; }
        public int keyPosition { get; set; }
        public int groupPosition { get; set; }
        public int sortPosition { get; set; }
        public int displayPosition { get; set; }
        public int ordinalPosition { get; set; }

        public bool isRequired { get; set; }

        public bool isCreated {
            get {return isToggleField("created");}
        }
        public bool isCreatedBy
        {
            get {return isToggleField("createdby", "created_by");}
        }

        public bool isUpdated
        {
            get {return isToggleField("updated");}
        }
        public bool isUpdatedBy
        {
            get { return isToggleField("updatedby", "updated_by");}
        }

        public bool isLocked
        {
            get { return isToggleField("locked"); }
        }
        public bool isLockedBy
        {
            get { return isToggleField("lockedby", "locked_by"); }
        }

        public bool isStampField
        {
            get { return isCreated || isCreatedBy || 
                    isLocked || isLockedBy || 
                    isUpdated || isUpdatedBy ||
                    keyPosition != -1; } 
        }

        private bool isToggleField(string compareName, string orCompareName = "")
        {
            if (name == null || prefix == null) return false;
            bool ret = name.ToLower() == (prefix.ToLower() + compareName);

            if (!ret && orCompareName.Length!=0)
                ret = name.ToLower() == (prefix.ToLower() + orCompareName);

            return ret;
        }

    }
}
