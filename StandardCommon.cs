using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventsHubSender
{
    public class StandardCommon
    {
        public string ClientId { get; set; }
        public long Imei { get; set; }
        public string Latitude { get; set; }
        public string Longitude { get; set; }
        public string GPSDate { get; set; }
        public int Heading { get; set; }
        public int EventID { get; set; }
        public string EventName { get; set; }
        public string AdditionalEventInfo { get; set; }
        public decimal Odometer { get; set; }
        public long? EngineHours { get; set; }
        public decimal VehicleSpeed { get; set; }
        public string RoadSpeedLimit { get; set; }
        public string RoadName { get; set; }
        public string GeoZoneID { get; set; }
        public int DriverID { get; set; }
        public bool IsIgnitionOn { get; set; }
        public string Altitude { get; set; }
        public string SensorData { get; set; }  //analog, 1-wire, serial
        public string CanBusData { get; set; }  //fuel etc
        public string Attributes { get; set; }  //Number Of Sats, GSM Strength, Car Battery power, internal tracker battery power
        public string CellTowerInfo { get; set; } //MMC, MNC, LAC, ID        
        public string RawData { get; set; }
        public string AssetName { get; set; }
    }
}
