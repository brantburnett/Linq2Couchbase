using System;
using System.Collections.Concurrent;
using System.Reflection;

namespace Couchbase.Linq.Analytics
{
    public class DefaultDataSetNameProvider : IDataSetNameProvider
    {
        private readonly ConcurrentDictionary<Type, string> _cache =
            new ConcurrentDictionary<Type, string>();

        public string GetDataSetName(Type type)
        {
            if (type == null)
            {
                throw new ArgumentNullException(nameof(type));
            }

            return _cache.GetOrAdd(type, key =>
            {
                var attribute = key.GetTypeInfo().GetCustomAttribute<DataSetAttribute>(true);

                return attribute?.Name ?? type.Name;
            });
        }
    }
}
