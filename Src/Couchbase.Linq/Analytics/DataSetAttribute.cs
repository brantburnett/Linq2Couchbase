using System;

namespace Couchbase.Linq.Analytics
{
    [AttributeUsage(AttributeTargets.Class)]
    public class DataSetAttribute : Attribute
    {
        public string Name { get; }

        public DataSetAttribute(string name)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentException("Value cannot be null or empty.", nameof(name));
            }

            Name = name;
        }
    }
}
