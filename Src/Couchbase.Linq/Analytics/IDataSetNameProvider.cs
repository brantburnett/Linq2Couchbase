using System;

namespace Couchbase.Linq.Analytics
{
    public interface IDataSetNameProvider
    {
        string GetDataSetName(Type type);
    }
}
