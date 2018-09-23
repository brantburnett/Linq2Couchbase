﻿using System;
using System.ComponentModel.DataAnnotations;
using Couchbase.Linq.Analytics;
using Newtonsoft.Json;

namespace Couchbase.Linq.IntegrationTests.Documents
{
    [DataSet("airports")]
    public class Airport
    {
        [Key]
        [JsonProperty("id")]
        public virtual int Id { get; set; }

        [JsonProperty("airportname")]
        public virtual string AirportName { get; set; }

        [JsonProperty("faa")]
        public virtual string Faa { get; set; }

        [JsonProperty("type")]
        public virtual string Type { get; set; }
    }
}
