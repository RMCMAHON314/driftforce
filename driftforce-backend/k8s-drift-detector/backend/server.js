const express = require('express');
const cors = require('cors');
const app = express();
const PORT = 3000;

app.use(cors());
app.use(express.json());

const generateDrifts = () => {
  const services = ['k8s-prod-cluster', 'prod-db-cluster-primary', 'cache-redis-01', 'api-gateway-sg-primary', 'queue-kafka-prod'];
  const severities = ['CRITICAL', 'WARNING'];
  const parameters = ['cpu', 'memory', 'replicas', 'version', 'env'];
  const drifts = [];
  const count = Math.floor(Math.random() * 5) + 3;
  
  for (let i = 0; i < count; i++) {
    drifts.push({
      name: `${parameters[Math.floor(Math.random() * parameters.length)].toUpperCase()} Configuration Drift`,
      severity: severities[Math.floor(Math.random() * severities.length)],
      description: `Resource configuration has deviated from baseline by ${Math.floor(Math.random() * 50 + 10)}%`,
      service: services[Math.floor(Math.random() * services.length)],
      firstSeen: `${Math.floor(Math.random() * 24)}h ${Math.floor(Math.random() * 60)}m ago`,
      impact: Math.random() > 0.5 ? 'HIGH' : 'MEDIUM',
      affected: `${Math.floor(Math.random() * 20 + 1)} instances`,
      parameter: parameters[Math.floor(Math.random() * parameters.length)],
      currentValue: `${Math.floor(Math.random() * 4000 + 1000)}m`
    });
  }
  return drifts;
};

app.get('/api/drifts', (req, res) => {
  res.json(generateDrifts());
});

app.get('/api/metrics', (req, res) => {
  res.json({
    scanRate: Math.floor(Math.random() * 50000 + 40000),
    detectionLatency: (Math.random() * 50 + 20).toFixed(1),
    resources: 96311,
    configsPerSec: Math.floor(Math.random() * 2000 + 1000),
    anomalyScore: (Math.random() * 5 + 1).toFixed(1),
    preventedLoss: Math.floor(Math.random() * 500000 + 500000),
    accuracy: (Math.random() * 5 + 95).toFixed(1)
  });
});

app.get('/', (req, res) => {
  res.json({ status: 'DriftForce Backend Running', version: '3.2.1' });
});

app.listen(PORT, () => {
  console.log(`
🌊 DriftForce Backend Started! 🌊
===================================
Server: http://localhost:${PORT}
API: http://localhost:${PORT}/api/drifts
===================================`);
});
