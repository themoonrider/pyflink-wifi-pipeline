# import json
import logging
from typing import Dict, List
from dataclasses import dataclass
from collections import deque
import numpy as np
# from scipy import stats
from config import config

logger = logging.getLogger(__name__)

@dataclass
class AnomalyResult:
    is_anomaly: bool
    anomaly_type: str
    severity: str  # "low", "medium", "high", "critical"
    confidence: float
    details: Dict
    router_id: str
    timestamp: str

class RouterState:
    """Maintains historical state for each router"""
    def __init__(self, router_id: str, window_size: int = 100):
        self.router_id = router_id
        self.window_size = window_size
        self.signal_history = deque(maxlen=window_size)
        self.speed_history = deque(maxlen=window_size)
        self.latency_history = deque(maxlen=window_size)
        self.packet_loss_history = deque(maxlen=window_size)
        self.baseline_calculated = False
        self.baseline_stats = {}
    
    def add_measurement(self, data: Dict):
        """Add new measurement to history"""
        self.signal_history.append(data.get("signal_strength", 0))
        self.speed_history.append(data.get("download_speed", 0))
        self.latency_history.append(data.get("latency", 0))
        self.packet_loss_history.append(data.get("packet_loss", 0))
        
        # Calculate baseline stats if we have enough data
        if len(self.signal_history) >= 30 and not self.baseline_calculated:
            self._calculate_baseline()
    
    def _calculate_baseline(self):
        """Calculate baseline statistics for normal operation"""
        self.baseline_stats = {
            "signal_mean": np.mean(list(self.signal_history)[-30:]),
            "signal_std": np.std(list(self.signal_history)[-30:]),
            "speed_mean": np.mean(list(self.speed_history)[-30:]),
            "speed_std": np.std(list(self.speed_history)[-30:]),
            "latency_mean": np.mean(list(self.latency_history)[-30:]),
            "latency_std": np.std(list(self.latency_history)[-30:]),
            "packet_loss_mean": np.mean(list(self.packet_loss_history)[-30:]),
            "packet_loss_std": np.std(list(self.packet_loss_history)[-30:])
        }
        self.baseline_calculated = True
        logger.info(f"Calculated baseline for router {self.router_id}")

class AnomalyDetector:
    def __init__(self):
        self.router_states: Dict[str, RouterState] = {}
        self.thresholds = config.thresholds
        
    def get_router_state(self, router_id: str) -> RouterState:
        """Get or create router state"""
        if router_id not in self.router_states:
            self.router_states[router_id] = RouterState(router_id)
        return self.router_states[router_id]
    
    def detect_threshold_anomalies(self, data: Dict) -> List[AnomalyResult]:
        """Detect anomalies based on absolute thresholds"""
        anomalies = []
        router_id = data.get("router_id")
        timestamp = data.get("timestamp")
        
        # Signal strength anomaly
        signal_strength = data.get("signal_strength", 0)
        if signal_strength < self.thresholds.signal_strength_min:
            severity = "critical" if signal_strength < -85 else "high"
            anomalies.append(AnomalyResult(
                is_anomaly=True,
                anomaly_type="weak_signal",
                severity=severity,
                confidence=0.95,
                details={
                    "current_signal": signal_strength,
                    "threshold": self.thresholds.signal_strength_min,
                    "deviation": abs(signal_strength - self.thresholds.signal_strength_min)
                },
                router_id=router_id,
                timestamp=timestamp
            ))
        
        # Speed degradation
        download_speed = data.get("download_speed", 0)
        if download_speed < 20:  # Less than 20 Mbps considered slow
            severity = "critical" if download_speed < 5 else "high"
            anomalies.append(AnomalyResult(
                is_anomaly=True,
                anomaly_type="slow_speed",
                severity=severity,
                confidence=0.90,
                details={
                    "current_speed": download_speed,
                    "threshold": 20
                },
                router_id=router_id,
                timestamp=timestamp
            ))
        
        # High latency
        latency = data.get("latency", 0)
        if latency > self.thresholds.latency_max:
            severity = "critical" if latency > 200 else "high"
            anomalies.append(AnomalyResult(
                is_anomaly=True,
                anomaly_type="high_latency",
                severity=severity,
                confidence=0.85,
                details={
                    "current_latency": latency,
                    "threshold": self.thresholds.latency_max
                },
                router_id=router_id,
                timestamp=timestamp
            ))
        
        # Packet loss
        packet_loss = data.get("packet_loss", 0)
        if packet_loss > self.thresholds.packet_loss_max:
            severity = "critical" if packet_loss > 10 else "high"
            anomalies.append(AnomalyResult(
                is_anomaly=True,
                anomaly_type="packet_loss",
                severity=severity,
                confidence=0.90,
                details={
                    "current_packet_loss": packet_loss,
                    "threshold": self.thresholds.packet_loss_max
                },
                router_id=router_id,
                timestamp=timestamp
            ))
        
        return anomalies
    
    def detect_statistical_anomalies(self, data: Dict) -> List[AnomalyResult]:
        """Detect anomalies using statistical methods (Z-score)"""
        anomalies = []
        router_id = data.get("router_id")
        timestamp = data.get("timestamp")
        
        router_state = self.get_router_state(router_id)
        
        # Add current measurement to history
        router_state.add_measurement(data)
        
        # Skip if baseline not calculated yet
        if not router_state.baseline_calculated:
            return anomalies
        
        baseline = router_state.baseline_stats
        
        # Signal strength Z-score
        signal_strength = data.get("signal_strength", 0)
        if baseline["signal_std"] > 0:
            signal_zscore = abs((signal_strength - baseline["signal_mean"]) / baseline["signal_std"])
            if signal_zscore > self.thresholds.z_score_threshold:
                anomalies.append(AnomalyResult(
                    is_anomaly=True,
                    anomaly_type="signal_anomaly",
                    severity="medium" if signal_zscore < 4 else "high",
                    confidence=min(0.95, signal_zscore / 5.0),
                    details={
                        "z_score": signal_zscore,
                        "current_value": signal_strength,
                        "baseline_mean": baseline["signal_mean"],
                        "baseline_std": baseline["signal_std"]
                    },
                    router_id=router_id,
                    timestamp=timestamp
                ))
        
        # Speed Z-score
        download_speed = data.get("download_speed", 0)
        if baseline["speed_std"] > 0:
            speed_zscore = abs((download_speed - baseline["speed_mean"]) / baseline["speed_std"])
            if speed_zscore > self.thresholds.z_score_threshold:
                anomalies.append(AnomalyResult(
                    is_anomaly=True,
                    anomaly_type="speed_anomaly",
                    severity="medium" if speed_zscore < 4 else "high",
                    confidence=min(0.95, speed_zscore / 5.0),
                    details={
                        "z_score": speed_zscore,
                        "current_value": download_speed,
                        "baseline_mean": baseline["speed_mean"],
                        "baseline_std": baseline["speed_std"]
                    },
                    router_id=router_id,
                    timestamp=timestamp
                ))
        
        return anomalies
    
    def detect_anomalies(self, data: Dict) -> List[AnomalyResult]:
        """Main anomaly detection method combining multiple techniques"""
        all_anomalies = []
        
        # Combine threshold-based and statistical anomalies
        all_anomalies.extend(self.detect_threshold_anomalies(data))
        all_anomalies.extend(self.detect_statistical_anomalies(data))
        
        # Remove duplicates and sort by severity
        unique_anomalies = []
        seen_types = set()
        
        for anomaly in sorted(all_anomalies, key=lambda x: {"critical": 4, "high": 3, "medium": 2, "low": 1}[x.severity], reverse=True):
            if anomaly.anomaly_type not in seen_types:
                unique_anomalies.append(anomaly)
                seen_types.add(anomaly.anomaly_type)
        
        return unique_anomalies