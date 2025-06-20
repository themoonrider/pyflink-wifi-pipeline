import json
import logging
from datetime import datetime

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema
from pyflink.datastream.functions import ProcessFunction, KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig
from pyflink.common import Types, Time
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.window import TumblingProcessingTimeWindows, SlidingProcessingTimeWindows

from anomaly_detector import AnomalyDetector
from config import config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/opt/logs/flink_processor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class RouterDataProcessor(KeyedProcessFunction):
    """Process function for individual router data processing"""
    
    def __init__(self):
        self.anomaly_detector = None
        self.router_state = None
    
    def open(self, runtime_context):
        """Initialize the process function"""
        self.anomaly_detector = AnomalyDetector()
        
        # Create state descriptor for router metrics history
        state_descriptor = ValueStateDescriptor(
            "router_metrics_history",
            Types.STRING()
        )
        
        # Configure state TTL (keep state for 24 hours)
        ttl_config = StateTtlConfig.new_builder(Time.hours(24)) \
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite) \
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired) \
            .build()
        state_descriptor.enable_time_to_live(ttl_config)
        
        self.router_state = runtime_context.get_state(state_descriptor)
    
    def process_element(self, value, ctx: 'ProcessFunction.Context', collector):
        """Process each router data point"""
        try:
            # Parse the input data
            data = json.loads(value)
            router_id = data.get("router_id")
            
            logger.debug(f"Processing data for router {router_id}")
            
            # Detect anomalies
            anomalies = self.anomaly_detector.detect_anomalies(data)
            
            # If anomalies detected, send alerts
            for anomaly in anomalies:
                alert = {
                    "alert_id": f"{router_id}_{int(datetime.now().timestamp())}",
                    "router_id": anomaly.router_id,
                    "timestamp": anomaly.timestamp,
                    "anomaly_type": anomaly.anomaly_type,
                    "severity": anomaly.severity,
                    "confidence": anomaly.confidence,
                    "details": anomaly.details,
                    "original_data": data
                }
                
                # Emit alert to output stream
                collector.collect(json.dumps(alert))
                
                logger.info(f"ANOMALY DETECTED - Router: {router_id}, Type: {anomaly.anomaly_type}, Severity: {anomaly.severity}")
            
            # Update router state (store recent metrics)
            current_state = self.router_state.value()
            if current_state:
                state_data = json.loads(current_state)
            else:
                state_data = {"recent_metrics": [], "baseline_calculated": False}
            
            # Add current metrics to state
            state_data["recent_metrics"].append({
                "timestamp": data.get("timestamp"),
                "signal_strength": data.get("signal_strength"),
                "download_speed": data.get("download_speed"),
                "latency": data.get("latency"),
                "packet_loss": data.get("packet_loss")
            })
            
            # Keep only last 100 measurements
            if len(state_data["recent_metrics"]) > 100:
                state_data["recent_metrics"] = state_data["recent_metrics"][-100:]
            
            # Update state
            self.router_state.update(json.dumps(state_data))
            
        except Exception as e:
            logger.error(f"Error processing router data: {e}")

class NetworkAggregator(ProcessFunction):
    """Aggregate network-wide statistics"""
    
    def process_element(self, value, ctx: 'ProcessFunction.Context', collector):
        """Process aggregated network data"""
        try:
            data = json.loads(value)
            
            # Simple aggregation - in production this would be more sophisticated
            network_summary = {
                "timestamp": datetime.now().isoformat(),
                "total_routers": 1,  # This would aggregate across all routers
                "avg_signal_strength": data.get("signal_strength", 0),
                "avg_download_speed": data.get("download_speed", 0),
                "network_health": "good" if data.get("signal_strength", 0) > -60 else "degraded"
            }
            
            collector.collect(json.dumps(network_summary))
            
        except Exception as e:
            logger.error(f"Error in network aggregation: {e}")

def create_kafka_source():
    """Create Kafka source for WiFi data"""
    return KafkaSource.builder() \
        .set_bootstrap_servers(config.kafka.bootstrap_servers) \
        .set_topics(config.kafka.wifi_data_topic) \
        .set_group_id(config.kafka.consumer_group) \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

def create_kafka_sink(topic: str):
    """Create Kafka sink for alerts"""
    return KafkaSink.builder() \
        .set_bootstrap_servers(config.kafka.bootstrap_servers) \
        .set_record_serializer(
            SimpleStringSchema()
        ) \
        .set_delivery_guarantee('at-least-once') \
        .build()

def main():
    """Main Flink application"""
    logger.info("Starting WiFi Network Monitoring Flink Job")
    
    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    
    # Enable checkpointing for fault tolerance
    env.enable_checkpointing(60000)  # Checkpoint every minute
    
    # Create Kafka source
    kafka_source = create_kafka_source()
    
    # Create data stream from Kafka
    wifi_data_stream = env.from_source(
        kafka_source,
        watermark_strategy=None,
        source_name="WiFi Data Source"
    )
    
    # Key by router_id for parallel processing
    keyed_stream = wifi_data_stream.key_by(
        lambda data: json.loads(data).get("router_id", "unknown")
    )
    
    # Process each router's data for anomaly detection
    alert_stream = keyed_stream.process(RouterDataProcessor())
    
    # Create Kafka sink for alerts
    kafka_sink = create_kafka_sink(config.kafka.alerts_topic)
    
    # Send alerts to Kafka
    alert_stream.sink_to(kafka_sink)
    
    # Create windowed aggregations
    windowed_stream = wifi_data_stream \
        .window(TumblingProcessingTimeWindows.of(Time.minutes(5))) \
        .process(NetworkAggregator())
    
    # Print aggregated results (in production, this would go to a dashboard)
    windowed_stream.print()
    
    # Execute the job
    logger.info("Executing Flink job...")
    env.execute("WiFi Network Monitoring")

if __name__ == "__main__":
    main()