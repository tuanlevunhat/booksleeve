using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BookSleeve
{
    public sealed class ErrorEventArgs : EventArgs
    {
        public Exception Exception { get; private set; }
        public string Cause { get; private set; }
        public bool IsFatal { get; private set; }
        internal ErrorEventArgs(Exception exception, string cause, bool isFatal)
        {
            this.Exception = exception;
            this.Cause = cause;
            this.IsFatal = isFatal;
        }
    }
}
