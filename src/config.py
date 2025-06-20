import os
from dataclasses import dataclass
from typing import Dict, List

@dataclass
class KafkaConfig:
    bootstrap_servers: str = "kafka:29092"
    wifi_data_topic: str = "wifi-router-data"
    alerts_topic: str = "network-alerts"
    consumer_group: str = "flink-processor"

@dataclass
class AnomalyThresholds:
    signal_strength_min: float = -70.0  # dBm
    speed_drop_percentage: float = 50.0
    latency_max: float = 100.0  # ms
    packet_loss_max: float = 5.0  # percentage
    z_score_threshold: float = 3.0

@dataclass
class WindowConfig:
    tumbling_window_seconds: int = 300  # 5 minutes
    sliding_window_seconds: int = 30
    slide_interval_seconds: int = 10

@dataclass
class Config:
    kafka: KafkaConfig = KafkaConfig()
    thresholds: AnomalyThresholds = AnomalyThresholds()
    windows: WindowConfig = WindowConfig()
    
    # Router locations (simulated ISP coverage area)
    router_locations: List[Dict] = None
    
    def __post_init__(self):
        if self.router_locations is None:
            self.router_locations = [
                {"lat": 3.0738, "lon": 101.5183, "area": "Klang"},
                {"lat": 3.1390, "lon": 101.6869, "area": "Kuala_Lumpur"},
                {"lat": 3.0600, "lon": 101.5040, "area": "Shah_Alam"},
                {"lat": 3.2028, "lon": 101.7337, "area": "Ampang"},
                {"lat": 3.0319, "lon": 101.4406, "area": "Subang"},
            ]

# Global config instance
config = Config()