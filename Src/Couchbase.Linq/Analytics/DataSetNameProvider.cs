using System;

namespace Couchbase.Linq.Analytics
{
    public static class DataSetNameProvider
    {
        public static IDataSetNameProvider Current { get; set; } =
            new DefaultDataSetNameProvider();
    }
}
