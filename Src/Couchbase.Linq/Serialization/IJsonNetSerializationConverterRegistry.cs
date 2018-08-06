﻿using System;
using Newtonsoft.Json;

namespace Couchbase.Linq.Serialization
{
    /// <summary>
    /// Registry of <see cref="ISerializationConverter"/> implementations based upon <see cref="JsonConverter"/>s.
    /// </summary>
    public interface IJsonNetSerializationConverterRegistry
    {
        /// <summary>
        /// Creates an instance of an <see cref="ISerializationConverter"/> implementation for
        /// a specific <see cref="JsonConverter"/>.  May return null.
        /// </summary>
        /// <param name="jsonConverter"><see cref="JsonConverter"/> to acquire</param>
        /// <returns>A new <see cref="ISerializationConverter"/>, or null if no converter is found.</returns>
        ISerializationConverter GetSerializationConverter(JsonConverter jsonConverter);
    }
}
