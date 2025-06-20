import json
import random
import time
import logging
from datetime import datetime, timezone
from typing import Dict, Any
from kafka import KafkaProducer
from config import config
import numpy as np

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/opt/logs/data_generator.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class WiFiDataGenerator:
    def __init__(self):
        self.producer = None
        self.router_count = 50
        self.base_metrics = self._initialize_router_baselines()
        self.anomaly_probability = 0.05  # 5% chance of anomaly per message
        
    def _initialize_router_baselines(self) -> Dict[str, Dict]:
        """Initialize baseline metrics for each router"""
        baselines = {}
        locations = config.router_locations
        
        for i in range(self.router_count):
            router_id = f"R{i:03d}"
            location = random.choice(locations)
            
            baselines[router_id] = {
                "location": location,
                "baseline_signal": random.uniform(-50, -30),
                "baseline_download": random.uniform(80, 120),
                "baseline_upload": random.uniform(10, 20),
                "baseline_latency": random.uniform(10, 25),
                "baseline_devices": random.randint(5, 15)
            }
        
        return baselines
    
    def _connect_kafka(self):
        """Connect to Kafka producer"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=config.kafka.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            logger.info("Connected to Kafka successfully")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
    
    def _generate_normal_data(self, router_id: str) -> Dict[str, Any]:
        """Generate normal operating data for a router"""
        baseline = self.base_metrics[router_id]
        
        # Add some realistic variance to baseline metrics
        signal_strength = baseline["baseline_signal"] + random.uniform(-5, 5)
        download_speed = baseline["baseline_download"] + random.uniform(-10, 10)
        upload_speed = baseline["baseline_upload"] + random.uniform(-2, 2)
        latency = baseline["baseline_latency"] + random.uniform(-3, 8)
        connected_devices = baseline["baseline_devices"] + random.randint(-2, 3)
        
        return {
            "router_id": router_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "signal_strength": round(signal_strength, 1),
            "download_speed": round(max(0, download_speed), 1),
            "upload_speed": round(max(0, upload_speed), 1),
            "latency": round(max(1, latency), 1),
            "packet_loss": round(random.uniform(0, 0.5), 2),
            "connected_devices": max(0, connected_devices),
            "location": baseline["location"],
            "status": "normal"
        }
    
    def _inject_anomaly(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Inject various types of anomalies"""
        anomaly_type = random.choice([
            "signal_drop", "speed_degradation", "high_latency", 
            "packet_loss_spike", "complete_outage"
        ])
        
        data["status"] = "anomaly"
        data["anomaly_type"] = anomaly_type
        
        if anomaly_type == "signal_drop":
            data["signal_strength"] = random.uniform(-90, -75)
        
        elif anomaly_type == "speed_degradation":
            # Reduce speed by 60-80%
            reduction_factor = random.uniform(0.2, 0.4)
            data["download_speed"] *= reduction_factor
            data["upload_speed"] *= reduction_factor
        
        elif anomaly_type == "high_latency":
            data["latency"] = random.uniform(100, 300)
        
        elif anomaly_type == "packet_loss_spike":
            data["packet_loss"] = random.uniform(5, 15)
        
        elif anomaly_type == "complete_outage":
            data["signal_strength"] = -100
            data["download_speed"] = 0
            data["upload_speed"] = 0
            data["latency"] = 1000
            data["packet_loss"] = 100
        
        return data
    
    def generate_data_stream(self):
        """Main data generation loop"""
        if not self.producer:
            self._connect_kafka()
        
        logger.info(f"Starting data generation for {self.router_count} routers")
        
        message_count = 0
        anomaly_count = 0
        
        try:
            while True:
                for router_id in self.base_metrics.keys():
                    # Generate normal data
                    data = self._generate_normal_data(router_id)
                    
                    # Randomly inject anomalies
                    if random.random() < self.anomaly_probability:
                        data = self._inject_anomaly(data)
                        anomaly_count += 1
                    
                    # Send to Kafka
                    self.producer.send(
                        config.kafka.wifi_data_topic,
                        key=router_id,
                        value=data
                    )
                    
                    message_count += 1
                    
                    # Log progress every 1000 messages
                    if message_count % 1000 == 0:
                        logger.info(f"Sent {message_count} messages, {anomaly_count} anomalies")
                
                # Flush and wait before next batch
                self.producer.flush()
                time.sleep(1)  # 1 second between batches
                
        except KeyboardInterrupt:
            logger.info("Data generation stopped by user")
        except Exception as e:
            logger.error(f"Error in data generation: {e}")
        finally:
            if self.producer:
                self.producer.close()

if __name__ == "__main__":
    generator = WiFiDataGenerator()
    generator.generate_data_stream()