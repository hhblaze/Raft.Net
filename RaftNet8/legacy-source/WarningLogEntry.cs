/* 
  Copyright (C) 2018 tiesky.com / Alex Solovyov
  It's a free software for those, who think that it should be free.
*/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raft
{
    public class WarningLogEntry
    {
        public enum eLogType
        {
            ERROR,
            WARNING,
            INFORMATION,
            DEBUG
        }

        public WarningLogEntry()
        {
            DateTime = DateTime.Now;
            LogType = eLogType.ERROR;
            Method = String.Empty;
            Description = String.Empty;
        }

        /// <summary>
        /// Full name namespace.{{class}}.method /property
        /// </summary>
        public string Method { get; set; }

        public string Description { get; set; }
        public Exception Exception { get; set; }

        /// <summary>
        /// Is set automatically to now from constructor
        /// </summary>
        public DateTime DateTime { get; set; }
        /// <summary>
        /// Default is ERROR
        /// </summary>
        public eLogType LogType { get; set; }

        public override string ToString()
        {
            return $"{this.DateTime.ToString("dd.MM.yyyy HH:mm:ss>")} [{this.LogType.ToString()}] [{Exception?.ToString()}] [{Method}] [{Description}]";
        }
    }
}
